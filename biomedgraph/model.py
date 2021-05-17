from graphio import NodeSet, RelationshipSet, ModelNode, ModelRelationship, MergeKey


class Result(ModelNode):
    name = MergeKey()


class Measurement(ModelNode):
    uid = MergeKey()


class Gene(ModelNode):
    sid = MergeKey()


class ResultToMeasurement(ModelRelationship):
    source = Result
    target = Measurement
    type = 'HAS_MEASUREMENT'


class MeasurementToGene(ModelRelationship):
    source = Measurement
    target = Gene
    type = 'MEASURES'
