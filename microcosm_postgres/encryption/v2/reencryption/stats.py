from dataclasses import dataclass


@dataclass
class ReencryptionStatistic:
    """
    Class usage to track reencryption statistics
    May be be used with the reencryption cli

    """
    model_name: str  # Name of the model being reencrypted
    total_instances_found: int  # Total number of instances found for the model
    instances_found_to_be_unencrypted: int  # Number of instances found to be unencrypted
    instances_reencrypted: int  # Number of instances reencrypted

    def log_stats(self):
        print(f"Model: {self.model_name}")  # noqa: T201
        print(f"- Total Instances Found: {self.total_instances_found}")  # noqa: T201
        print(f"- Instances Found to be Unencrypted: {self.instances_found_to_be_unencrypted}")  # noqa: T201
        print(f"- Instances Reencrypted: {self.instances_reencrypted}")  # noqa: T201
