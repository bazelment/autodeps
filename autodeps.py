#!/usr/bin/env python3

"""Suggest deps to use based on classes imported.

It requires a classes database that can be built with indexer.py
"""
import argparse
import gzip
import json
import logging
import os.path
import subprocess

logger = logging.getLogger(__name__)


class AutoDeps(object):
    def __init__(self, db_file):
        with gzip.open(db_file, "rt") as fp:
            db = json.load(fp)
        self.alias = db["alias"]
        # Reverse mapping from a rule to the possible alias
        self.rule_to_alias = dict()
        for a, r in self.alias.items():
            self.rule_to_alias[r] = a
        self.rules = db["jvm_libs"]
        self.class_to_rule = dict()
        for rule in self.rules.values():
            name = rule[0]
            classes = rule[4]
            for c in classes:
                if c in self.class_to_rule:
                    self.class_to_rule[c].append(name)
                    continue
                    logger.warning("Duplicate class detected: %s, in %s and %s",
                                   c, self.class_to_rule[c], name)

                else:
                    self.class_to_rule[c] = [name]

    def _get_sources(self, target):
        output = subprocess.check_output(
            ['bazel', 'cquery', '--output=jsonproto',
             'labels(srcs, {})'.format(target)],
            universal_newlines=True)
        l = json.loads(output)
        for item in l["results"]:
            target = item["target"]
            if target["type"] == "SOURCE_FILE":
                f = target["sourceFile"]["location"]
                yield f.split(':')[0]

    def _get_imports_from_file(self, fname):
        with open(fname, "r") as fp:
            for line in fp.readlines():
                if line.startswith("import "):
                    tokens = line.strip().split(maxsplit=1)
                    class_name = tokens[1]
                    if "{" in class_name:
                        # Deal with multiple imports, like import com.foo.bar.{A, B}
                        parts = class_name.split("{")
                        more_classes = parts[1].rstrip("}").split(",")
                        for i in more_classes:
                            yield parts[0] + i.strip()
                    else:
                        yield class_name

    def _find_bazel_rule_for_class(self, c):
        if c in self.class_to_rule:
            return self.class_to_rule[c]
        return []

    def _maybe_get_classes(self, target):
        if "." in target and not ":" in target:
            return set([target])
        src_files = list(self._get_sources(target))
        logger.info("Get sources %s", src_files)
        all_classes = set()
        for src in src_files:
            for c in self._get_imports_from_file(src):
                all_classes.add(c)
        return all_classes

    def resolve(self, target):
        """Resolve dependencies for the given bazel target.

        This is done by:
        1. Get the list of source files from the target.
        2. Get the classes imported by these source files.
        3. For each class, find the list of bazel targets that could
        provide the given class, sort them based on visibility and
        other restrictions(like skiplist, etc), choose the one that
        ranks highest.
        4. Merge all the resolved targets.
        5. Replace the target name with their alias if alias is set.
        """
        all_classes = self._maybe_get_classes(target)
        logger.info("classes %s", all_classes)
        # rule_name(that provides class) -> list of classes that need this rule.
        deps = dict()
        for c in all_classes:
            for d in self._find_bazel_rule_for_class(c):
                if d in deps:
                    deps[d].append(c)
                else:
                    deps[d] = [c]
        # Check exports to find the right deps.
        import pdb
        # pdb.set_trace()
        for d, classes in sorted(deps.items()):
            if d in self.rule_to_alias:
                d = self.rule_to_alias[d]
            print("# {}".format(" ".join(classes)))
            print('"{}",'.format(d))


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, description=__doc__
    )
    parser.add_argument(
        "target",
        type=str,
        default="//common:common",
        help="Which bazel target(//foo:bar) or class(com.foo.Bar) to process."
    )
    parser.add_argument(
        "--db",
        type=str,
        default="~/.cache/autodeps/autodeps-db.json.gz",
        help="Database used by autodeps",
    )

    args = parser.parse_args()
    a = AutoDeps(os.path.expanduser(args.db))
    a.resolve(args.target)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(levelname)s %(asctime)s.%(msecs)03d %(filename)s:%(lineno)d] %(message)s',
        datefmt='%m%d %H:%M:%S',
        level=logging.INFO)
    main()
