#!/usr/bin/python
"""Pdrf scraper"""

import logging
import os
from typing import Dict, List
from urllib.parse import urlencode

import requests
from geopandas import read_file
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_layers(self) -> List:
        """
        Get available services from ArcGIS server
        Return layers in each service
        """
        base_url = self._configuration["base_url"]
        folder = self._configuration["folder"]
        response = requests.get(f"{base_url}/{folder}?f=json").json()
        services = response.get("services", [])

        results = []
        for service in services:
            service_name = service["name"]
            service_type = service["type"]
            service_url = f"{base_url}/{service_name}/{service_type}"

            layers_response = requests.get(f"{service_url}?f=json").json()
            layers = layers_response.get("layers", [])
            if not layers:
                continue

            for layer in layers:
                layer_id = layer["id"]
                layer_name = layer["name"]
                layer_url = f"{service_url}/{layer_id}"

                try:
                    stats = self.get_date_range(layer_url)
                except Exception as e:
                    stats = {"min_date": None, "max_date": None}
                    print(f"Stats failed for {layer_url}: {e}")

                results.append(
                    {
                        "layer_id": layer_id,
                        "layer_name": layer_name,
                        "min_date": stats["min_date"],
                        "max_date": stats["max_date"],
                        "service_url": service_url,
                    }
                )

        return results

    def generate_dataset(self, layer_info: Dict):
        """
        Get layer data from ArcGIS API and create data outputs for HDX
        Return dataset
        """
        layer_id = layer_info["layer_id"]
        layer_name = layer_info["layer_name"]
        slug = slugify(layer_name)
        service_url = layer_info["service_url"]
        layer_url = f"{service_url}/{layer_id}"

        query = {
            "f": "json",
            "orderByFields": "OBJECTID",
            "outFields": "*",
            "where": "1=1",
        }
        query_url = f"{layer_url}/query?{urlencode(query)}"
        gdf = read_file("ESRIJSON:" + query_url)

        # Dataset info
        dataset = Dataset({"name": slug, "title": f"Philippines: {layer_name}"})
        dataset.set_time_period(layer_info["min_date"], layer_info["max_date"])
        dataset.add_tags(self._configuration["tags"])
        dataset_country_iso3 = "PHL"

        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Create GeoJSON
        output_dir = self._tempdir
        os.makedirs(output_dir, exist_ok=True)
        gdf.to_file(os.path.join(output_dir, f"{slug}.geojson"), driver="GeoJSON")

        # Flatten geometry and drop unnecessary columns for csv
        gdf["lon"] = gdf.geometry.x
        gdf["lat"] = gdf.geometry.y
        gdf.drop(columns=["geometry", "ObjectID_1"]).fillna("")

        # Add csv resource
        layer_data = gdf.to_dict(orient="records")
        resource_description = f"CSV format of the summary of {layer_name}"
        resource_data = {"name": f"{slug}.csv", "description": resource_description}
        dataset.generate_resource_from_iterable(
            headers=list(layer_data[0].keys()),
            iterable=layer_data,
            hxltags={},
            folder=self._tempdir,
            filename=f"{slug}.csv",
            resourcedata=resource_data,
            quickcharts=None,
        )

        # Add geojson resource
        geojson_resource = Resource(
            {
                "name": f"{slug}.geojson",
                "description": f"Geojson format of the summary of {layer_name}",
                "format": "GeoJSON",
            }
        )
        geojson_path = os.path.join(output_dir, f"{slug}.geojson")
        geojson_resource.set_file_to_upload(geojson_path)
        dataset.add_update_resource(geojson_resource)

        # Add geoservice resource
        geoservice_resource = {
            "name": layer_name,
            "description": f"ArcGIS Map Service of the summary of {layer_name}",
            "url": service_url,
            "format": "GeoService",
        }
        dataset.add_update_resource(geoservice_resource)

        return dataset

    def get_date_range(self, layer_url: str) -> Dict:
        """
        Get min & max dates using outStatistics from ArcGIS API
        """
        date_field = "Date_of_Assistance_Deployment"  # date column from API
        stats = [
            {
                "statisticType": "min",
                "onStatisticField": date_field,
                "outStatisticFieldName": "min_date",
            },
            {
                "statisticType": "max",
                "onStatisticField": date_field,
                "outStatisticFieldName": "max_date",
            },
        ]

        stats_query = {
            "f": "json",
            "outFields": "*",
            "outStatistics": stats,
            "returnGeometry": "false",
            "where": "1=1",
        }
        stats_response = requests.get(
            f"{layer_url}/query?{urlencode(stats_query)}"
        ).json()

        attrs = stats_response["features"][0]["attributes"]
        date_fmt = "%m/%d/%Y %I:%M:%S %p"
        min_date = parse_date(attrs.get("min_date"), date_fmt)
        max_date = parse_date(attrs.get("max_date"), date_fmt)

        return {"min_date": min_date, "max_date": max_date}
