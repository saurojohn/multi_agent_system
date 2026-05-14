"""Task pipeline for chaining and composing complex workflows."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

logger = logging.getLogger('pipeline')


class PipelineState(Enum):
    """Pipeline execution state."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineStage:
    """A stage in the pipeline."""
    stage_id: str
    name: str
    task_type: str
    handler: Callable = None
    input_mapping: Dict[str, str] = field(default_factory=dict)  # output_field -> input_field
    output_mapping: Dict[str, str] = field(default_factory=dict)
    retry_count: int = 0
    timeout: int = 300
    condition: Callable = None  # Optional condition to skip stage


@dataclass
class PipelineExecution:
    """Execution context for a pipeline."""
    execution_id: str
    pipeline_id: str
    state: PipelineState
    current_stage: int = 0
    results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    end_time: float = None
    metadata: Dict = field(default_factory=dict)


@dataclass
class Pipeline:
    """A task pipeline definition."""
    pipeline_id: str
    name: str
    stages: List[PipelineStage]
    description: str = ""
    parallel: bool = False  # Execute stages in parallel if True
    continue_on_error: bool = False


class TaskPipeline:
    """
    Pipeline executor for chained tasks.
    """

    def __init__(self, orchestrator=None):
        self.orchestrator = orchestrator
        self._pipelines: Dict[str, Pipeline] = {}
        self._executions: Dict[str, PipelineExecution] = {}
        self._lock = threading.RLock()
        self._executors: Dict[str, Callable] = {}

    def register_pipeline(self, pipeline: Pipeline):
        """Register a pipeline definition."""
        with self._lock:
            self._pipelines[pipeline.pipeline_id] = pipeline
            logger.info(f"Registered pipeline: {pipeline.name}")

    def create_pipeline(self, name: str, stages: List[PipelineStage],
                        **config) -> Pipeline:
        """Create and register a new pipeline."""
        pipeline_id = str(uuid.uuid4())
        pipeline = Pipeline(
            pipeline_id=pipeline_id,
            name=name,
            stages=stages,
            **config
        )
        self.register_pipeline(pipeline)
        return pipeline

    def execute(self, pipeline_id: str, initial_input: Dict,
               execution_id: str = None) -> PipelineExecution:
        """Execute a pipeline."""
        with self._lock:
            if pipeline_id not in self._pipelines:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            execution_id = execution_id or str(uuid.uuid4())
            execution = PipelineExecution(
                execution_id=execution_id,
                pipeline_id=pipeline_id,
                state=PipelineState.RUNNING
            )
            self._executions[execution_id] = execution

        # Run pipeline in background
        thread = threading.Thread(
            target=self._execute_pipeline,
            args=(pipeline_id, execution_id, initial_input)
        )
        thread.start()

        return execution

    def _execute_pipeline(self, pipeline_id: str, execution_id: str, initial_input: Dict):
        """Execute pipeline stages."""
        pipeline = self._pipelines[pipeline_id]
        execution = self._executions[execution_id]

        try:
            current_results = initial_input

            for stage_idx, stage in enumerate(pipeline.stages):
                execution.current_stage = stage_idx

                # Check condition
                if stage.condition and not stage.condition(current_results):
                    logger.info(f"Stage {stage.name} skipped due to condition")
                    continue

                # Map input
                stage_input = self._map_input(stage.input_mapping, current_results)

                # Execute stage
                try:
                    if stage.handler:
                        result = stage.handler(stage_input)
                    elif self.orchestrator:
                        # Submit to orchestrator
                        task_id = self.orchestrator.submit_task(
                            task_type=stage.task_type,
                            task_data=stage_input,
                            timeout=stage.timeout
                        )
                        # Wait for result (simplified)
                        result = {'task_id': task_id, 'status': 'submitted'}
                    else:
                        result = {'error': 'No handler or orchestrator'}

                    # Map output
                    mapped_result = self._map_output(stage.output_mapping, result)
                    current_results.update(mapped_result)
                    execution.results[stage.stage_id] = mapped_result

                except Exception as e:
                    logger.error(f"Stage {stage.name} failed: {e}")
                    execution.errors[stage.stage_id] = str(e)

                    if not pipeline.continue_on_error:
                        execution.state = PipelineState.FAILED
                        execution.end_time = time.time()
                        return

                # Small delay between stages
                time.sleep(0.01)

            execution.state = PipelineState.COMPLETED
            logger.info(f"Pipeline {pipeline.name} completed: {execution_id}")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            execution.state = PipelineState.FAILED
            execution.errors['pipeline'] = str(e)

        execution.end_time = time.time()

    def _map_input(self, mapping: Dict[str, str], source: Dict) -> Dict:
        """Map input fields from source."""
        if not mapping:
            return source

        result = {}
        for output_field, input_field in mapping.items():
            result[output_field] = source.get(input_field, source.get(output_field))
        return result

    def _map_output(self, mapping: Dict[str, str], source: Dict) -> Dict:
        """Map output fields to results."""
        if not mapping:
            return source

        result = {}
        for input_field, output_field in mapping.items():
            result[output_field] = source.get(input_field, source.get(output_field))
        return result

    def get_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Get execution by ID."""
        with self._lock:
            return self._executions.get(execution_id)

    def list_executions(self, pipeline_id: str = None) -> List[PipelineExecution]:
        """List executions, optionally filtered by pipeline."""
        with self._lock:
            executions = list(self._executions.values())
            if pipeline_id:
                executions = [e for e in executions if e.pipeline_id == pipeline_id]
            return executions

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        with self._lock:
            if execution_id in self._executions:
                exec = self._executions[execution_id]
                if exec.state == PipelineState.RUNNING:
                    exec.state = PipelineState.CANCELLED
                    exec.end_time = time.time()
                    return True
        return False

    def get_stats(self) -> Dict:
        """Get pipeline statistics."""
        with self._lock:
            return {
                'total_pipelines': len(self._pipelines),
                'total_executions': len(self._executions),
                'running': sum(1 for e in self._executions.values() if e.state == PipelineState.RUNNING),
                'completed': sum(1 for e in self._executions.values() if e.state == PipelineState.COMPLETED),
                'failed': sum(1 for e in self._executions.values() if e.state == PipelineState.FAILED)
            }


class PipelineBuilder:
    """Builder for creating pipelines programmatically."""

    def __init__(self, name: str):
        self._name = name
        self._stages: List[PipelineStage] = []
        self._description: str = ""
        self._parallel: bool = False
        self._continue_on_error: bool = False

    def add_stage(self, name: str, task_type: str,
                  handler: Callable = None, **config) -> 'PipelineBuilder':
        """Add a stage to the pipeline."""
        stage_id = str(uuid.uuid4())
        stage = PipelineStage(
            stage_id=stage_id,
            name=name,
            task_type=task_type,
            handler=handler,
            **{k: v for k, v in config.items() if v is not None}
        )
        self._stages.append(stage)
        return self

    def map_input(self, output_field: str, input_field: str) -> 'PipelineBuilder':
        """Add input mapping to the last stage."""
        if self._stages:
            self._stages[-1].input_mapping[output_field] = input_field
        return self

    def map_output(self, input_field: str, output_field: str) -> 'PipelineBuilder':
        """Add output mapping to the last stage."""
        if self._stages:
            self._stages[-1].output_mapping[input_field] = output_field
        return self

    def with_retry(self, count: int) -> 'PipelineBuilder':
        """Set retry count for last stage."""
        if self._stages:
            self._stages[-1].retry_count = count
        return self

    def with_timeout(self, timeout: int) -> 'PipelineBuilder':
        """Set timeout for last stage."""
        if self._stages:
            self._stages[-1].timeout = timeout
        return self

    def with_condition(self, condition: Callable) -> 'PipelineBuilder':
        """Set condition for last stage."""
        if self._stages:
            self._stages[-1].condition = condition
        return self

    def description(self, desc: str) -> 'PipelineBuilder':
        """Set pipeline description."""
        self._description = desc
        return self

    def parallel(self, enabled: bool = True) -> 'PipelineBuilder':
        """Enable parallel execution."""
        self._parallel = enabled
        return self

    def continue_on_error(self, enabled: bool = True) -> 'PipelineBuilder':
        """Continue on error."""
        self._continue_on_error = enabled
        return self

    def build(self, pipeline_id: str = None) -> Pipeline:
        """Build the pipeline."""
        return Pipeline(
            pipeline_id=pipeline_id or str(uuid.uuid4()),
            name=self._name,
            stages=self._stages,
            description=self._description,
            parallel=self._parallel,
            continue_on_error=self._continue_on_error
        )


# Global pipeline executor
_pipeline_executor = TaskPipeline()


def get_pipeline_executor() -> TaskPipeline:
    return _pipeline_executor


def create_pipeline(name: str) -> PipelineBuilder:
    """Create a new pipeline builder."""
    return PipelineBuilder(name)