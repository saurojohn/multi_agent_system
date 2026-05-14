"""Task approval workflow for high-value tasks."""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field

logger = logging.getLogger('approval')


class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass
class ApprovalRequest:
    request_id: str
    task_id: str
    task_type: str
    task_data: Dict
    requester: str
    approver: Optional[str] = None
    status: ApprovalStatus = ApprovalStatus.PENDING
    reason: str = ""
    created_at: float = field(default_factory=time.time)
    decided_at: Optional[float] = None
    comments: List[str] = field(default_factory=list)


class ApprovalWorkflow:
    """
    Manages approval workflow for high-value or risky tasks.
    Tasks requiring approval are held until approved or rejected.
    """

    def __init__(self,
                 approval_timeout: float = 3600.0,  # 1 hour default
                 auto_approve_threshold: float = 0.0):  # 0 = all need approval
        self.approval_timeout = approval_timeout
        self.auto_approve_threshold = auto_approve_threshold

        self._pending_approvals: Dict[str, ApprovalRequest] = {}
        self._completed_approvals: Dict[str, ApprovalRequest] = {}
        self._approval_callbacks: Dict[str, Callable] = {}
        self._running = False
        self._expiry_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    def set_approval_callback(self, task_type: str, callback: Callable):
        """Set callback to invoke when task is approved."""
        self._approval_callbacks[task_type] = callback

    def request_approval(self, task_id: str, task_type: str,
                       task_data: Dict, requester: str,
                       reason: str = "") -> str:
        """Request approval for a task."""
        request_id = f"approval-{uuid.uuid4().hex[:8]}"

        approval_req = ApprovalRequest(
            request_id=request_id,
            task_id=task_id,
            task_type=task_type,
            task_data=task_data,
            requester=requester,
            reason=reason
        )

        with self._lock:
            self._pending_approvals[request_id] = approval_req

        logger.info(f'Approval requested: {request_id} for task {task_id} (type: {task_type})')
        return request_id

    def approve(self, request_id: str, approver: str, comments: str = "") -> bool:
        """Approve a pending request."""
        with self._lock:
            if request_id not in self._pending_approvals:
                return False

            req = self._pending_approvals[request_id]
            req.status = ApprovalStatus.APPROVED
            req.approver = approver
            req.decided_at = time.time()
            if comments:
                req.comments.append(comments)

            # Move to completed
            self._completed_approvals[request_id] = req
            del self._pending_approvals[request_id]

        logger.info(f'Approval granted: {request_id} by {approver}')

        # Invoke callback
        if req.task_type in self._approval_callbacks:
            try:
                self._approval_callbacks[req.task_type](req)
            except Exception as e:
                logger.error(f'Approval callback failed: {e}')

        return True

    def reject(self, request_id: str, approver: str,
              reason: str = "", comments: str = "") -> bool:
        """Reject a pending request."""
        with self._lock:
            if request_id not in self._pending_approvals:
                return False

            req = self._pending_approvals[request_id]
            req.status = ApprovalStatus.REJECTED
            req.approver = approver
            req.reason = reason
            req.decided_at = time.time()
            if comments:
                req.comments.append(comments)

            # Move to completed
            self._completed_approvals[request_id] = req
            del self._pending_approvals[request_id]

        logger.info(f'Approval rejected: {request_id} by {approver} (reason: {reason})')
        return True

    def get_pending(self) -> List[ApprovalRequest]:
        """Get all pending approval requests."""
        with self._lock:
            return list(self._pending_approvals.values())

    def get_request(self, request_id: str) -> Optional[ApprovalRequest]:
        """Get a specific approval request."""
        with self._lock:
            if request_id in self._pending_approvals:
                return self._pending_approvals[request_id]
            if request_id in self._completed_approvals:
                return self._completed_approvals[request_id]
        return None

    def is_approved(self, task_id: str) -> Optional[bool]:
        """
        Check if task is approved.
        Returns: True if approved, False if rejected, None if pending/not found.
        """
        with self._lock:
            for req in self._pending_approvals.values():
                if req.task_id == task_id:
                    return None  # Still pending

            for req in self._completed_approvals.values():
                if req.task_id == task_id:
                    return req.status == ApprovalStatus.APPROVED

        return None

    def start(self):
        """Start expiry monitoring."""
        if self._running:
            return
        self._running = True
        self._expiry_thread = threading.Thread(target=self._expiry_loop, daemon=True)
        self._expiry_thread.start()
        logger.info('Approval workflow started')

    def stop(self):
        """Stop expiry monitoring."""
        self._running = False
        if self._expiry_thread:
            self._expiry_thread.join(timeout=5)
        logger.info('Approval workflow stopped')

    def _expiry_loop(self):
        """Background thread to expire old approvals."""
        while self._running:
            now = time.time()

            with self._lock:
                expired = []
                for req_id, req in self._pending_approvals.items():
                    if now - req.created_at > self.approval_timeout:
                        req.status = ApprovalStatus.EXPIRED
                        req.decided_at = now
                        expired.append(req_id)

                for req_id in expired:
                    req = self._pending_approvals[req_id]
                    self._completed_approvals[req_id] = req
                    del self._pending_approvals[req_id]
                    logger.warning(f'Approval expired: {req_id}')

            time.sleep(60)

    def get_stats(self) -> Dict:
        """Get approval workflow statistics."""
        with self._lock:
            pending = len(self._pending_approvals)
            approved = sum(1 for r in self._completed_approvals.values()
                          if r.status == ApprovalStatus.APPROVED)
            rejected = sum(1 for r in self._completed_approvals.values()
                          if r.status == ApprovalStatus.REJECTED)
            expired = sum(1 for r in self._completed_approvals.values()
                         if r.status == ApprovalStatus.EXPIRED)

            return {
                'pending': pending,
                'approved': approved,
                'rejected': rejected,
                'expired': expired,
                'total': pending + approved + rejected + expired
            }


class ApprovalMiddleware:
    """Middleware to integrate approval workflow with task submission."""

    def __init__(self, workflow: ApprovalWorkflow = None,
                 approval_required: Callable[[str, Dict], bool] = None):
        self._workflow = workflow or ApprovalWorkflow()
        self._approval_required = approval_required or (lambda t, d: False)

    def should_require_approval(self, task_type: str, task_data: Dict) -> bool:
        """Check if task requires approval."""
        return self._approval_required(task_type, task_data)

    def submit_with_approval(self, orchestrator, task_type: str,
                            task_data: Dict, requester: str = "system") -> Dict:
        """
        Submit task with approval workflow.
        Returns dict with 'status' ('pending_approval' or 'task_id'),
        'approval_request_id' or 'task_id'.
        """
        if self.should_require_approval(task_type, task_data):
            # Create approval request
            request_id = self._workflow.request_approval(
                task_id="",  # Will be generated on approval
                task_type=task_type,
                task_data=task_data,
                requester=requester
            )
            return {
                'status': 'pending_approval',
                'approval_request_id': request_id
            }
        else:
            # Submit directly
            task_id = orchestrator.submit_task(task_type, task_data)
            return {
                'status': 'submitted',
                'task_id': task_id
            }


# Global approval workflow
_approval_workflow = ApprovalWorkflow()


def get_approval_workflow() -> ApprovalWorkflow:
    return _approval_workflow