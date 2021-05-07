from graphio import NodeSet, RelationshipSet


class ModelObject:
    pass


class ModelNode(ModelObject):
    labels = []
    merge_keys = []

    @classmethod
    def dataset(cls):
        return NodeSet(cls.labels, merge_keys=cls.merge_keys)


class ModelRelationship(ModelObject):
    source = None
    target = None
    type = ''

    @classmethod
    def dataset(cls):
        return RelationshipSet(cls.type, cls.source.labels, cls.target.labels, cls.source.merge_keys, cls.target.merge_keys)


class Analysis(ModelNode):
    labels = ['Result']
    merge_keys = ['uid']


class Result(ModelNode):
    labels = ['Result']
    merge_keys = ['uid']


class Measurement(ModelNode):
    labels = ['Measurement']
    merge_keys = ['uid']


class AnalysisToResult(ModelRelationship):
    source = Analysis
    target = Result
    type = 'HAS_RESULT'
