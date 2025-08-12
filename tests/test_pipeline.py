from datetime import datetime, timezone
from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.pdrf.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestPdrf",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                layers = pipeline.get_layers()
                assert layers[0] == {
                    "layer_id": 0,
                    "layer_name": "TC_CARINA_PS_Response",
                    "max_date": datetime(2024, 8, 5, 4, 0, tzinfo=timezone.utc),
                    "min_date": datetime(2024, 7, 24, 4, 0, tzinfo=timezone.utc),
                    "service_url": "https://handa.pdrf.org.ph/arcgis/rest/services/2024_PS_Response_Efforts/TC_2024_000127_PHL_PDRF_PSResponse/MapServer",
                }

                dataset = pipeline.generate_dataset(layers[0])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": None,
                    "data_update_frequency": -1,
                    "dataset_date": "[2024-07-24T00:00:00 TO 2024-08-05T23:59:59]",
                    "dataset_source": "Philippine Disaster Resilience Foundation",
                    "groups": [{"name": "phl"}],
                    "license_id": "cc-by",
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "methodology": "Registry",
                    "name": "tc-carina-ps-response",
                    "notes": 'Private Sector\'s Involvement ("Who is doing What Where")',
                    "owner_org": "c9acf432-a02a-4342-ba62-d22acca0af97",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "tags": [
                        {
                            "name": "cyclones-hurricanes-typhoons",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "disaster risk reduction-drr",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Philippines: TC_CARINA_PS_Response",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "CSV format of the summary of TC_CARINA_PS_Response",
                        "format": "csv",
                        "name": "tc-carina-ps-response.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Geojson format of the summary of TC_CARINA_PS_Response",
                        "format": "geojson",
                        "name": "tc-carina-ps-response.geojson",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "ArcGIS Map Service of the summary of TC_CARINA_PS_Response",
                        "format": "geoservice",
                        "name": "TC_CARINA_PS_Response",
                        "resource_type": "api",
                        "url": "https://handa.pdrf.org.ph/arcgis/rest/services/2024_PS_Response_Efforts/TC_2024_000127_PHL_PDRF_PSResponse/MapServer",
                        "url_type": "api",
                    },
                ]
