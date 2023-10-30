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


class ReencryptionStatsCollector:
    """
    Used in the collection of reencryption statistics
    Designed to be used with the reencryption cli

    """
    def __init__(self):
        self.instances_found_to_be_unencrypted = 0
        self.instances_reencrypted = 0
        self.total_instances_found = 0
        self.model = None

        # Data holds a mapping of model_name to ReencryptionStatistic
        self.data = dict()

    def update(self, found_to_be_unencrypted, changed_committed, model_name: str):
        statistic = self.data.get(model_name)
        if statistic is None:
            statistic = ReencryptionStatistic(
                model_name=model_name,
                total_instances_found=0,
                instances_found_to_be_unencrypted=0,
                instances_reencrypted=0,
            )
            self.data[model_name] = statistic

        statistic.instances_found_to_be_unencrypted += 1 if found_to_be_unencrypted else 0
        statistic.instances_reencrypted += 1 if changed_committed else 0
        statistic.total_instances_found += 1

    def get_stats(self) -> list[ReencryptionStatistic]:
        return list(self.data.values())
