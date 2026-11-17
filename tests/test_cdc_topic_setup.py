"""Guard against losing Kafka CLI flags in Docker Compose argument parsing."""

import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
TOPIC = "portpulse_stream.stream_demo.port_eta_state"


class KafkaTopicSetupTests(unittest.TestCase):
    def test_topic_setup_passes_flags_as_executable_arguments(self):
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
        services = compose["services"]
        setup = services["cdc-topic-setup"]

        self.assertEqual(setup["entrypoint"], ["/opt/kafka/bin/kafka-topics.sh"])
        self.assertEqual(
            setup["command"],
            [
                "--bootstrap-server",
                "kafka:9092",
                "--create",
                "--if-not-exists",
                "--topic",
                TOPIC,
                "--partitions",
                "3",
                "--replication-factor",
                "1",
            ],
        )
        self.assertEqual(
            services["cdc-setup"]["depends_on"]["cdc-topic-setup"]["condition"],
            "service_completed_successfully",
        )


if __name__ == "__main__":
    unittest.main()
