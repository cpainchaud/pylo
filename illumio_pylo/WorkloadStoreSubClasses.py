from dataclasses import dataclass
from typing import Optional, List, Iterable, Any

from .API.JsonPayloadTypes import WorkloadObjectCreateJsonStructure, WorkloadInterfaceObjectJsonStructure
from .LabeledObject import LabeledObject
import illumio_pylo as pylo


class UnmanagedWorkloadDraft(LabeledObject):

    @dataclass
    class DraftInterface:
        def __init__(self, ip: str, name: Optional[str] = "umw"):
            self.name: str = name
            self.ip: str = ip


    def __init__(self, owner: 'pylo.WorkloadStore', name: Optional[str] = None, hostname: Optional[str] = None,
                 description: Optional[str] = None, ip_single_or_list: Optional[str|Iterable[str]] = None,
                 labels: Optional[List['pylo.Label']] = None):
        super().__init__()
        self.owner = owner
        self.name: Optional[str] = name
        self.hostname: Optional[str] = hostname
        self.description: Optional[str] = description
        self.interfaces: List[UnmanagedWorkloadDraft.DraftInterface] = []
        if ip_single_or_list is not None:
            if isinstance(ip_single_or_list, str):
                self.add_interface(ip_single_or_list)
            else:
                for ip in ip_single_or_list:
                    self.add_interface(ip)
        if labels is not None:
            for label in labels:
                self.set_label(label)

    def add_interface(self, ip: str, name: Optional[str] = "umw"):
        self.interfaces.append(UnmanagedWorkloadDraft.DraftInterface(ip, name))

    def generate_json_payload(self) -> WorkloadObjectCreateJsonStructure:
        return WorkloadObjectCreateJsonStructure(
            name=self.name if self.name is not None else self.hostname,
            hostname=self.hostname,
            description=self.description if self.description is not None else "",
            interfaces=self._generate_interfaces_json_payload(),
            labels=[{"href": label.href} for label in self._labels.values()],
        )

    def _generate_interfaces_json_payload(self) -> List[WorkloadInterfaceObjectJsonStructure]:
        results = []
        for interface in self.interfaces:
            results.append(WorkloadInterfaceObjectJsonStructure(name=interface.name, address=interface.ip))
        return results

    def create_in_pce(self) -> pylo.Workload:
        api = self.owner.owner.connector
        api_results = api.objects_workload_create_single_unmanaged(self.generate_json_payload())
        return self.owner.add_workload_from_json(api_results)

@dataclass
class CreationTracker:
    draft: UnmanagedWorkloadDraft
    external_tracker_id: Any
    success: bool
    message: str
    workload: Optional[pylo.Workload] = None
    workload_href: Optional[str] = None


class UnmanagedWorkloadDraftMultiCreatorManager:
    def __init__(self, owner: 'pylo.WorkloadStore'):


        self.owner = owner
        self.drafts: List[UnmanagedWorkloadDraft] = []
        self._external_tracker_ids: List[Any] = []

    def count_drafts(self) -> int:
        return len(self.drafts)

    def new_draft(self, external_tracker_id: Any = None) -> UnmanagedWorkloadDraft:
        draft = UnmanagedWorkloadDraft(self.owner)
        self.drafts.append(draft)
        self._external_tracker_ids.append(external_tracker_id)
        return draft

    def create_all_in_pce(self, amount_created_per_batch = 500, retrieve_workloads_after_creation = False) -> List[CreationTracker]:
        results: List[CreationTracker] = []

        if retrieve_workloads_after_creation:
            for draft in self.drafts:
                try:
                    new_workload = draft.create_in_pce()
                    results.append(CreationTracker(draft, self._external_tracker_ids[self.drafts.index(draft)], True, "Success",
                                                   workload=new_workload))
                except pylo.PyloApiEx as e:
                    results.append(CreationTracker(draft, self._external_tracker_ids[self.drafts.index(draft)], False, str(e)))

            return results


        batches: List[List[UnmanagedWorkloadDraft]] = []

        # slice the drafts into arrays of arrays of size=amount_created_per_batch
        for i in range(0, len(self.drafts), amount_created_per_batch):
            batches.append(self.drafts[i:i + amount_created_per_batch])

        for batch in batches:
            multi_create_payload: List[WorkloadObjectCreateJsonStructure] = []
            for draft in batch:
                multi_create_payload.append(draft.generate_json_payload())

            api = self.owner.owner.connector

            try :
                api_results = api.objects_workload_create_bulk_unmanaged(multi_create_payload)
                for i in range(len(api_results)):
                    if api_results[i]["status"] == "created":
                        results.append(CreationTracker(draft=batch[i],
                                                       external_tracker_id=self._external_tracker_ids[self.drafts.index(batch[i])],
                                                       success=True,
                                                       message="Success",
                                                       workload_href=api_results[i]["href"]))
                    else:
                        results.append(CreationTracker(draft=batch[i],
                                                       external_tracker_id=self._external_tracker_ids[self.drafts.index(batch[i])],
                                                       success=False,
                                                       message=api_results[i]["message"]))
            except pylo.PyloApiEx as e:
                for draft in batch:
                    results.append(CreationTracker(draft=draft,
                                                   external_tracker_id=self._external_tracker_ids[self.drafts.index(draft)],
                                                   success=False,
                                                   message=str(e)))




        return results