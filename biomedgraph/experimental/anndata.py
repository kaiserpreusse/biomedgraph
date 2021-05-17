import logging

from anndata import AnnData
import pandas as pd
from graphio import ModelNode, ModelRelationship, Label, MergeKey, Container
import numpy as np
from enum import Enum
from uuid import uuid4
from typing import Union, Type


log = logging.getLogger(__name__)


def load_anndata(anndata: AnnData, observation: Type[ModelNode],
                 observation_to_measurement: Type[ModelRelationship], measurement: Type[ModelNode],
                 measurement_to_target: Type[ModelRelationship], target: Union[str, Type[ModelNode]],
                 target_property: str = None) -> Container:
    """
    Load an AnnData object with target entities in columns and :Analysis in rows.


    """
    output = Container()
    measurements = measurement.dataset()
    results = observation.dataset()
    result_to_measurement = observation_to_measurement.dataset()

    if isinstance(target, str):
        class TargetObject(ModelNode):
            label = Label(target)
            key = MergeKey(target_property)

        target = TargetObject

        # if no target_property is provided, try to get the first __merge_key__ element
    if len(target.__merge_keys__) > 1 and not target_property:
        raise TypeError("AnnData loader currently only works for target nodes with one specific property. "
                        "Pass a class with exactly one MergeKey or define one in the function call.")

    if not target_property:
        target_property = target.__merge_keys__[0]

    # overwrite target node on measurement_to_target relationship
    measurement_to_target.target = target

    measurement_to_target_data = measurement_to_target.dataset()

    output.add_all([measurements, results, result_to_measurement, measurement_to_target_data])

    for obs_name in anndata.obs_names:
        results.add_node({observation.name: obs_name})
        for feature, obs in anndata[obs_name].to_df().iteritems():
            value = obs[0]
            # log.debug(f"obs_name: {obs_name}, feature: {feature}, value: {value}")
            measurement_uid = str(uuid4())
            measurements.add_node({measurement.uid: measurement_uid, 'value': float(value)})
            result_to_measurement.add_relationship({observation.name: obs_name}, {measurement.uid: measurement_uid}, {})
            measurement_to_target_data.add_relationship({measurement.uid: measurement_uid}, {target_property: feature}, {})

    return output
