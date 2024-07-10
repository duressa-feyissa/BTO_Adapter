import rdflib
from rdflib import Graph


class BTOAdapter:
    def __init__(self, ontology_path, dbsnp_rsid_map=None, dbsnp_pos_map=None, write_properties=True, add_provenance=True):
        self.ontology_path = ontology_path
        self.dbsnp_rsid_map = dbsnp_rsid_map
        self.dbsnp_pos_map = dbsnp_pos_map
        self.write_properties = write_properties
        self.add_provenance = add_provenance
        self.graph = Graph()
        self.graph.parse(ontology_path, format="xml")

    def get_nodes(self):
        nodes = []
        for s, p, o in self.graph:
            if p == rdflib.namespace.RDFS.label:
                node = {
                    'id': str(s),
                    'label': str(o),
                    'properties': {
                        'uri': str(s)
                    }
                }
                nodes.append(node)
        return nodes

    def get_edges(self):
        edges = []
        for s, p, o in self.graph:
            edge = {
                'source': str(s),
                'target': str(o),
                'label': str(p),
                'properties': {
                    'uri': str(p)
                }
            }
            edges.append(edge)
        return edges
