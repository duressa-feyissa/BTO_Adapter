import os
import pathlib

import networkx as nx
from biocypher import BioCypher
from biocypher._logger import logger


class MeTTaWriter:

    def __init__(self, schema_config, biocypher_config, output_dir):
        self.schema_config = schema_config
        self.biocypher_config = biocypher_config
        self.output_path = pathlib.Path(output_dir)

        if not os.path.exists(output_dir):
            self.output_path.mkdir()

        # Ensure the _config dictionary is properly initialized
        if "_config" not in globals():
            globals()["_config"] = {}

        for key, value in biocypher_config.items():
            if key not in globals()["_config"]:
                globals()["_config"][key] = value
            else:
                globals()["_config"][key].update(value)

        self.bcy = BioCypher(schema_config_path=schema_config)

        self.ontology = self.bcy._get_ontology()
        self.create_type_hierarchy()

        self.excluded_properties = []

    def create_type_hierarchy(self):
        G = self.ontology._nx_graph
        file_path = f"{self.output_path}/type_defs.metta"
        with open(file_path, "w") as f:
            for node in G.nodes:
                if "mixin" in node:
                    continue
                ancestor = list(self.get_parent(G, node))[-1]
                node = self.convert_input_labels(node)
                ancestor = self.convert_input_labels(ancestor)
                if ancestor == node:
                    f.write(f"(: {node.upper()} Type)\n")
                else:
                    f.write(f"(<: {node.upper()} {ancestor.upper()})\n")

            self.create_data_constructors(f)

        logger.info("Type hierarchy created successfully.")

    def create_data_constructors(self, file):
        schema = self.bcy._get_ontology_mapping()._extend_schema()
        self.edge_node_types = {}

        def edge_data_constructor(edge_type, source_type, target_type, label):
            return f"(: {label.lower()} (-> {source_type.upper()} {target_type.upper()} {edge_type.upper()}))"

        def node_data_constructor(node_type, node_label):
            return f"(: {node_label.lower()} (-> $x {node_type.upper()}))"

        for k, v in schema.items():
            if v["represented_as"] == "edge":
                edge_type = self.convert_input_labels(k)
                source_type = v.get("source", None)
                target_type = v.get("target", None)
                if source_type is not None and target_type is not None:
                    if isinstance(v["input_label"], list):
                        label = self.convert_input_labels(v["input_label"][0])
                        source_type = self.convert_input_labels(source_type[0])
                        target_type = self.convert_input_labels(target_type[0])
                    else:
                        label = self.convert_input_labels(v["input_label"])
                        source_type = self.convert_input_labels(source_type)
                        target_type = self.convert_input_labels(target_type)

                    output_label = v.get("output_label", None)
                    out_str = edge_data_constructor(edge_type, source_type, target_type, label)
                    file.write(out_str + "\n")
                    self.edge_node_types[label.lower()] = {
                        "source": source_type.lower(),
                        "target": target_type.lower(),
                        "output_label": output_label.lower() if output_label is not None else None
                    }

            elif v["represented_as"] == "node":
                label = v["input_label"]
                if not isinstance(label, list):
                    label = [label]

                label = [self.convert_input_labels(l) for l in label]
                node_type = self.convert_input_labels(k)
                for l in label:
                    out_str = node_data_constructor(node_type, l)
                    file.write(out_str + "\n")

    def print_nodes(self, nodes, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/nodes.metta"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"{self.output_path}/nodes.metta"
        for node in nodes:
            out_str = self.write_node(node)
            for s in out_str:
                print(s)

        print()

        logger.info("Finished writing out nodes")

    def print_edges(self, edges, path_prefix=None, create_dir=True):
        if path_prefix is not None:
            file_path = f"{self.output_path}/{path_prefix}/edges.metta"
            if create_dir:
                if not os.path.exists(f"{self.output_path}/{path_prefix}"):
                    pathlib.Path(f"{self.output_path}/{path_prefix}").mkdir(parents=True, exist_ok=True)
        else:
            file_path = f"{self.output_path}/edges.metta"

        for edge in edges:
            print("Processing edge:", edge)  # Debug statement
            out_str = self.write_edge(edge)
            for s in out_str:
                print(s)

        print()

    def write_node(self, node):
        id, label, properties = node
        if "." in label:
            label = label.split(".")[1]
        def_out = f"({self.convert_input_labels(label)} {id})"
        return self.write_property(def_out, properties)

    def write_edge(self, edge):
        source_id, target_id, label, properties = edge
        label = label.lower()
        print(f"Edge label: {label}")  # Debug statement
        if label not in self.edge_node_types:
            raise KeyError(f"Label '{label}' not found in edge_node_types")  # Debug statement
        source_type = self.edge_node_types[label]["source"]
        target_type = self.edge_node_types[label]["target"]
        output_label = self.edge_node_types[label]["output_label"]
        if output_label is not None:
            label = output_label
        def_out = f"({label} ({source_type} {source_id}) ({target_type} {target_id}))"
        return self.write_property(def_out, properties)

    def write_property(self, def_out, property):
        out_str = [def_out]
        if isinstance(property, dict):
            for k, v in property.items():
                if k in self.excluded_properties or v is None or v == "":
                    continue
                if isinstance(v, list):
                    prop = "("
                    for i, e in enumerate(v):
                        prop += f'{self.check_property(e)}'
                        if i != len(v) - 1:
                            prop += " "
                    prop += ")"
                    out_str.append(f'({k} {def_out} {prop})')
                elif isinstance(v, dict):
                    prop = f"({k} {def_out})"
                    out_str.extend(self.write_property(prop, v))
                else:
                    out_str.append(f'({k} {def_out} {self.check_property(v)})')
        return out_str

    def check_property(self, prop):
        if isinstance(prop, str):
            if " " in prop:
                prop = prop.replace(" ", "_")

            special_chars = ["(", ")"]
            escape_char = "\\"
            return "".join(escape_char + c if c in special_chars or c == escape_char else c for c in prop)

        return prop

    def convert_input_labels(self, label, replace_char="_"):
        """
        A method that removes spaces in input labels and replaces them with replace_char
        :param label: Input label of a node or edge
        :param replace_char: the character to replace spaces with
        :return:
        """
        return label.replace(" ", replace_char)

    def get_parent(self, G, node):
        """
        Get the immediate parent of a node in the ontology.
        """
        return nx.dfs_preorder_nodes(G, node, depth_limit=2)

    def show_ontology_structure(self):
        self.bcy.show_ontology_structure()

    def summary(self):
        self.bcy.summary()
