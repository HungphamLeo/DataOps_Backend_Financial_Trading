"""
dataops_processing.core — Core Architecture Engine (Orchestrator, Base Job, Registry).
"""
from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.orchestrator import ProcessingOrchestrator, get_orchestrator
from dataops_processing.core.registry import JobRegistry

__all__ = [
    "BaseProcessingJob",
    "JobRegistry",
    "ProcessingOrchestrator",
    "get_orchestrator",
]
