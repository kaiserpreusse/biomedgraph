from uuid import uuid4
import logging
from anndata import AnnData
import pandas as pd
from graphio import ModelNode, ModelRelationship, Label, MergeKey, Container
import numpy as np
from enum import Enum
from uuid import uuid4
from typing import Union, Type

log = logging.getLogger(__name__)


class ResultSet:
    """
    Container for a set of related results which logically fit into one AnnData object.

    ResultSets can be chained to create a sequence of data analysis steps.
    """

    def __init__(self, name: str = None, anndata: AnnData = None, observation: Type[ModelNode] = None,
                 observation_to_measurement: Type[ModelRelationship] = None, measurement: Type[ModelNode] = None,
                 measurement_to_target: Type[ModelRelationship] = None, target: Union[str, Type[ModelNode]] = None,
                 target_property: str = None):

        self.uid = str(uuid4())
        self.name = name
        self._data = anndata
        self.observation = observation
        self.observation_to_measurement = observation_to_measurement
        self.measurement = measurement
        self.measurement_to_target = measurement_to_target
        self.target = None

        self._container = Container()
        self.set_target(target, target_property)

        self.parents = []

    def set_target(self, target: Union[str, Type[ModelNode]], target_property: str = None):
        log.info(f"Set target to {target}, {target_property}")
        if isinstance(target, str):
            class TargetObject(ModelNode):
                label = Label(target)
                key = MergeKey(target_property)

            self.target = TargetObject
        elif issubclass(target, ModelNode):
            # if no target_property is provided, try to get the first __merge_key__ element
            if len(target.__merge_keys__) > 1 and not target_property:
                raise TypeError("AnnData loader currently only works for target nodes with one specific property. "
                                "Pass a class with exactly one MergeKey or define one in the function call.")
            self.target = target

    @property
    def obs(self):
        return self._data.obs

    def parse(self, graph):
        """
        Load an AnnData object with target entities in columns and :Analysis in rows.
        """

        df = self._data.to_df()

        for row in df.iterrows():

            container = Container()
            measurements = self.measurement.dataset()
            results = self.observation.dataset()
            result_to_measurement = self.observation_to_measurement.dataset()

            target_property = self.target.__merge_keys__[0]

            # overwrite target node on measurement_to_target relationship
            self.measurement_to_target.target = self.target

            measurement_to_target_data = self.measurement_to_target.dataset()

            container.add_all([measurements, results, result_to_measurement, measurement_to_target_data])
            log.info(f"Parse {str(self)}")

            obs_name = row[0]
            log.debug(f"Parse {obs_name}")
            results.add_node({self.observation.name: obs_name})

            for feature, value in row[1].iteritems():
                measurement_uid = str(uuid4())
                measurements.add_node({self.measurement.uid: measurement_uid, 'value': float(value)})
                result_to_measurement.add_relationship({self.observation.name: obs_name}, {self.measurement.uid: measurement_uid},
                                                       {})
                measurement_to_target_data.add_relationship({self.measurement.uid: measurement_uid},
                                                            {target_property: feature}, {})

            for ns in container.nodesets:
                ns.create_index(graph)
                ns.merge(graph)

            for rs in container.relationshipsets:
                rs.create_index(graph)
                rs.merge(graph)



        # for obs_name in self._data.obs_names:
        #     log.debug(f"Parse {obs_name}")
        #     results.add_node({self.observation.name: obs_name})
        #
        #     for feature, value in zip(self._data.var_names, self._data[obs_name].X[0]):
        #         # log.debug(f"obs_name: {obs_name}, feature: {feature}, value: {value}")
        #         measurement_uid = str(uuid4())
        #         measurements.add_node({self.measurement.uid: measurement_uid, 'value': float(value)})
        #         result_to_measurement.add_relationship({self.observation.name: obs_name}, {self.measurement.uid: measurement_uid},
        #                                                {})
        #         measurement_to_target_data.add_relationship({self.measurement.uid: measurement_uid},
        #                                                     {target_property: feature}, {})

            # for feature, obs in self._data[obs_name].to_df().iteritems():
            #     value = obs[0]
            #     # log.debug(f"obs_name: {obs_name}, feature: {feature}, value: {value}")
            #     measurement_uid = str(uuid4())
            #     measurements.add_node({self.measurement.uid: measurement_uid, 'value': float(value)})
            #     result_to_measurement.add_relationship({self.observation.name: obs_name}, {self.measurement.uid: measurement_uid},
            #                                            {})
            #     measurement_to_target_data.add_relationship({self.measurement.uid: measurement_uid},
            #                                                 {target_property: feature}, {})

    def load(self, graph):
        """
        Load output to graph.

        :param graph: py2neo.Graph instance.
        """
        for ns in self._container.nodesets:
            ns.create_index(graph)
            ns.merge(graph)

        for rs in self._container.relationshipsets:
            rs.create_index(graph)
            rs.merge(graph)
