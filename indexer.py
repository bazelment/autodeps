#!/usr/bin/env python3

"""Prototype auto deps."""

import argparse
import collections
import gzip
import json
import logging
import subprocess
import os
import zipfile


logger = logging.getLogger(__name__)


# Represent any kind of JVM like library, like java_import,
# generic_scala_worker. The jar should be pointed to either ijar or
# the deploy jar(for library), which contains the list of class files.
class JvmLib(collections.namedtuple("JvmLib", [
        "name", "jars", "exports", "visibility", "classes"])):

    def add_class(self, c):
        self.classes.append(c)


def build_attributes_dict(rule):
    ret = {}
    for item in rule["attribute"]:
        if item["name"] == "actual":
            ret["actual"] = item["stringValue"]
        elif item["name"] == "visibility" and "stringListValue" in item:
            ret["visibility"] = item["stringListValue"]
        elif item["name"] == "exports" and "stringListValue" in item:
            ret["exports"] = item["stringListValue"]
        elif item["name"] == "srcs" and "stringListValue" in item:
            ret["src"] = item["stringListValue"]
        elif item["name"] == "jars" and "stringListValue" in item:
            ret["jars"] = item["stringListValue"]
        elif item["name"] == "emit_ijar" and "stringValue" in item:
            ret["emit_ijar"] = item["stringValue"] == "true"

    return ret


def get_class_names_from_jar(jar_file):
    """Get list of classes from jar by listing the class names."""
    with zipfile.ZipFile(jar_file) as zp:
        for name in zp.namelist():
            if name.endswith(".class"):
                yield name.replace("/", ".").removesuffix(".class")


class BazelWrapper(object):

    def __init__(self, workspace):
        # Workspace directory
        self.workspace = os.path.expanduser(workspace) if workspace else os.getcwd()

    def build(self, *targets):
        command = ["bazel", "build"] + list(targets)
        logger.info(" ".join(command))
        subprocess.check_call(["bazel", "build"] + list(targets),
                              cwd=self.workspace)

    def check_output(self, *args, **kwargs):
        """run bazel with the given args, return the output."""
        command = ["bazel"] + list(args)
        logger.info("Running %s", " ".join(command))
        return subprocess.check_output(command, cwd=self.workspace,
                                       universal_newlines=True,
                                       **kwargs)

    def cquery(self, *args, **kwargs):
        return self.check_output("cquery", *args, **kwargs)

    def get_deps_tree(self, target):
        return self.cquery("--noimplicit_deps",
                           "--output=jsonproto",
                           "deps({})".format(target))

    def get_info(self):
        output = self.check_output("info", "bazel-bin", "output_base")
        bazel_bin = None
        output_base = None
        for line in output.split("\n"):
            items = line.split(": ")
            if len(items) == 2:
                if items[0] == "bazel-bin":
                    bazel_bin = items[1].strip()
                elif items[0] == "output_base":
                    output_base = items[1].strip()
        return bazel_bin, output_base

    def get_sources(self, target):
        """Get the source files of the given target."""
        output = self.cquery(
            '--output=jsonproto',
            'labels(srcs, {})'.format(target),
            stderr=subprocess.DEVNULL)
        l = json.loads(output)
        if "results" in l:
            for item in l["results"]:
                target = item["target"]
                if target["type"] == "SOURCE_FILE":
                    f = target["sourceFile"]["location"]
                    yield f.split(':')[0]

    def get_outputs(self, target, suffix=None):
        """Get the outputs of the given target.

        If suffix is not None, only return the file with the given
        suffix.
        """
        expr = "'\\n'.join([f.path for f in target.files.to_list()])"
        cquery_output = self.cquery("--output=starlark",
                                    "--starlark:expr",
                                    expr, target, stderr=subprocess.DEVNULL)
        files = cquery_output.strip().split("\n")
        if suffix:
            return [x for x in files if x.endswith(suffix)]
        else:
            return files


class DepsParser(object):
    def __init__(self, bazel_wrapper):
        # Map from alias to the real target
        self.alias_map = dict()
        self.jvm_libs = dict()
        self.bazel = bazel_wrapper
        self.bazel_bin, self.output_base = self.bazel.get_info()
        self.skipped_rule_classes = set()
        logger.info("bazel-bin: %s, output_base: %s", self.bazel_bin, self.output_base)

    def parse(self, deps_json):
        # Parse the list of jvm related rules
        objs = json.loads(deps_json)
        for t in objs["results"]:
            target = t["target"]
            if target["type"] != "RULE":
                continue
            rule = target["rule"]
            if rule["ruleClass"] == "alias":
                attr = build_attributes_dict(rule)
                a = rule["name"]
                print("Alias {} -> {}".format(a, attr["actual"]))
                self.alias_map[a] = attr["actual"]
            elif rule["ruleClass"] == "generic_scala_worker":
                self._parse_scala_worker(rule)
            elif rule["ruleClass"] == "java_library":
                self._parse_java_library(rule)
            elif rule["ruleClass"] == "java_import":
                self._parse_java_import(rule)
            elif rule["ruleClass"] in ["scala_proto_library", "jarjar_links"]:
                self._parse_jar_generators(rule)
            else:
                self.skipped_rule_classes.add(rule["ruleClass"])
        # Get the list of classes from each rule
        self._scan_classes()

    def report(self):
        logger.info("Ignored these rule classes: %s", self.skipped_rule_classes)

    def _parse_scala_worker(self, rule):
        name = rule["name"]
        attr = build_attributes_dict(rule)
        emit_ijar = attr.get("emit_ijar", True)
        # skip ijar business, too complicated
        emit_ijar = False
        output = rule["ruleOutput"]
        jar = None
        for i in output:
            # Prefer _ijar for Scala if emit_ijar is True
            if emit_ijar:
                if i.endswith("_ijar.jar"):
                    jar = i
                    break
            else:
                if i.endswith("_deploy.jar"):
                    jar = i
                    break
        jar = self._guess_jar_full_path(jar)

        self.jvm_libs[name] = JvmLib(name, [jar], attr.get("exports", None),
                                     attr.get("visibility", None), [])

    def _parse_jar_generators(self, rule):
        """Get the output jars from the given rule.

        Certain rules generate jar file(s) without declaring them as
        output, ideally we should use JavaInfo provider to the
        rule/target to get such information.
        At the moment we just use `files` attribute.
        """
        name = rule["name"]
        attr = build_attributes_dict(rule)
        jars = self.bazel.get_outputs(name, suffix=".jar")
        self.jvm_libs[name] = JvmLib(name, jars, attr.get("exports", None),
                                     attr.get("visibility", None), [])

    def _parse_java_library(self, rule):
        name = rule["name"]
        attr = build_attributes_dict(rule)
        output = rule["ruleOutput"]
        jar = None
        for i in output:
            if i.endswith(".jar") and not i.endswith("-src.jar"):
                jar = i
                break
        jar = self._guess_jar_full_path(jar)
        self.jvm_libs[name] = JvmLib(name, [jar], attr.get("exports", None),
                                     attr.get("visibility", None), [])

    def _parse_java_import(self, rule):
        name = rule["name"]
        attr = build_attributes_dict(rule)
        jars = attr.get("jars", [])
        # The jars of java_import could be jar file in the source
        # tree, or output from another rule.
        resolved_jars = []
        if jars:
            # First we use basic transformation to get the full path
            # of each jars.
            for jar in jars:
                full_path = self._guess_jar_full_path(jar)
                if full_path:
                    resolved_jars.append(full_path)
        if len(resolved_jars) != len(jars) or len(jars) == 0:
            # Use expensive query to get jar paths
            resolved_jars = self.bazel.get_outputs(name, suffix=".jar")
        self.jvm_libs[name] = JvmLib(
            name, resolved_jars,
            attr.get("exports", None),
            attr.get("visibility", None), [])

    def _get_full_path_under_output(self, relative_path):
        # logger.info("search %s", relative_path)
        for prefix in [self.bazel.workspace, self.output_base, self.bazel_bin]:
            p = os.path.join(prefix, relative_path)
            logger.info("check %s", p)
            if os.path.exists(p):
                return p
        return None

    def _guess_jar_full_path(self, jar):
        # Start with basic transformation to get the jar location
        if jar.startswith("//"):
            # This is a rule inside the current workspace
            jar_relative_path = jar.removeprefix("//").replace(":", "/")
            gp = self._get_full_path_under_output(jar_relative_path)
            if gp:
                return gp
        elif jar.startswith("@"):
            # This is a rule from external repo, like '@scala_2_12//:lib/jline-2.14.6.jar'
            gp = self._get_full_path_under_output(os.path.join("external", jar.removeprefix("@").replace("//:", "/").replace("//", "/").replace(":", "/")))
            if gp:
                return gp
        elif jar.startswith("bazel-out"):
            # These are the jars got from generators
            gp = self._get_full_path_under_output(jar)
            if gp:
                return gp
        return None

    def _record_classes_from_rule(self, rule, jar):
        # jar_file = self._get_jar_location(jar, rule.name)
        # Some rule like java_import may have resolved the jar to full path already.
        if os.path.isabs(jar):
            jar_file = jar
        else:
            jar_file = self._get_full_path_under_output(jar)
        logger.info("jar is found in %s", jar_file)
        for c in get_class_names_from_jar(jar_file):
            rule.add_class(c)

    def _scan_classes(self):
        # Rules that should be skipped
        RULE_SKIP_LIST = [
            # Compile time only dependencies, only used for debezium
            "@debezium_1_7//:compile_time_only_dependencies",
        ]
        for rule in self.jvm_libs.values():
            if rule.name in RULE_SKIP_LIST:
                logger.info("Skip %s", rule.name)
                continue
            logger.info("check %s with %s", rule.name, rule.jars)
            for jar in rule.jars:
                self._record_classes_from_rule(rule, jar)

    def to_json(self):
        return json.dumps(
            dict(alias=self.alias_map,
                jvm_libs=self.jvm_libs))


class Indexer(object):

    def __init__(self, seed_target, seed_file, workspace):
        self.seed_target = seed_target
        self.seed_file = seed_file
        self.bazel = BazelWrapper(workspace)

    def bazel_output(self, *args):
        command = ["bazel"] + list(args)
        logger.info("Running %s", " ".join(command))
        return subprocess.check_output(command, cwd=self.universe, universal_newlines=True)

    def refresh(self, output):
        dep_parser = DepsParser(self.bazel)
        if self.seed_file:
            logger.info("loading from %s", self.seed_file)
            with open(os.path.expanduser(self.seed_file), "r") as fp:
                all_deps = fp.read()
            dep_parser.parse(all_deps)
        else:
            self.bazel.build(self.seed_target)
            all_deps = self.bazel.get_deps_tree(self.seed_target)
            dep_parser.parse(all_deps)
        dep_parser.report()
        output_file = os.path.expanduser(output)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with gzip.open(output_file, "wt") as fp:
            fp.write(dep_parser.to_json())
        logger.info("autodeps database is available in %s", output)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, description=__doc__
    )
    parser.add_argument(
        "--seed",
        type=str,
        default="//common:common",
        help="Initial bazel target(s) to discover all the relevant java/scala rules",
    )
    parser.add_argument(
        "--seed-file",
        type=str,
        default=None,
        help="If set, the json file that contains the list of build rule to start from",
    )
    parser.add_argument(
        "--workspace",
        type=str,
        default=None,
        help="Directory of bazel workspace(if unset, CWD will be used)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="~/.cache/autodeps/autodeps-db.json.gz",
        help="File to write generated database file"
    )
    args = parser.parse_args()
    i = Indexer(args.seed, args.seed_file, args.workspace)
    i.refresh(args.output)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(levelname)s %(asctime)s.%(msecs)03d %(filename)s:%(lineno)d] %(message)s',
        datefmt='%m%d %H:%M:%S',
        level=logging.INFO)
    main()
