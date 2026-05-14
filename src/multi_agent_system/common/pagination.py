"""Pagination utilities for API responses."""

import logging
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('pagination')


class PageType(Enum):
    """Pagination types."""
    OFFSET = "offset"      # offset/limit pagination
    CURSOR = "cursor"      # cursor-based pagination
    KEYSET = "keyset"      # keyset pagination (seek method)


@dataclass
class PageRequest:
    """A pagination request."""
    page: int = 1
    page_size: int = 20
    max_page_size: int = 100
    sort_by: str = None
    sort_order: str = "asc"  # asc or desc


@dataclass
class PageInfo:
    """Information about the current page."""
    page: int
    page_size: int
    total_items: int
    total_pages: int
    has_next: bool
    has_prev: bool
    first_item: int = None
    last_item: int = None


@dataclass
class Page[T]:
    """A page of results."""
    items: List[T]
    page_info: PageInfo
    metadata: Dict = field(default_factory=dict)


@dataclass
class CursorInfo:
    """Cursor information for cursor pagination."""
    next_cursor: str = None
    prev_cursor: str = None
    has_more: bool = False


class OffsetPaginator:
    """Offset-based pagination."""

    def __init__(self, max_page_size: int = 100, default_page_size: int = 20):
        self.max_page_size = max_page_size
        self.default_page_size = default_page_size

    def create_request(self, page: int = 1, page_size: int = None,
                      sort_by: str = None, sort_order: str = "asc") -> PageRequest:
        """Create a page request."""
        page_size = page_size or self.default_page_size
        page_size = min(page_size, self.max_page_size)

        return PageRequest(
            page=max(1, page),
            page_size=page_size,
            max_page_size=self.max_page_size,
            sort_by=sort_by,
            sort_order=sort_order
        )

    def paginate(self, items: List[Any], request: PageRequest) -> Page:
        """Paginate a list of items."""
        total_items = len(items)
        total_pages = max(1, (total_items + request.page_size - 1) // request.page_size)

        # Calculate offset
        offset = (request.page - 1) * request.page_size
        end = min(offset + request.page_size, total_items)

        # Sort if needed
        sorted_items = items
        if request.sort_by:
            reverse = request.sort_order == "desc"
            sorted_items = sorted(items, key=lambda x: self._get_sort_key(x, request.sort_by), reverse=reverse)

        # Slice
        page_items = sorted_items[offset:end]

        # Calculate first/last item indices
        first_item = offset + 1 if total_items > 0 else None
        last_item = end if total_items > 0 else None

        page_info = PageInfo(
            page=request.page,
            page_size=request.page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=request.page < total_pages,
            has_prev=request.page > 1,
            first_item=first_item,
            last_item=last_item
        )

        return Page(items=page_items, page_info=page_info)

    def _get_sort_key(self, item: Any, sort_by: str):
        """Get sort key from item."""
        if hasattr(item, sort_by):
            return getattr(item, sort_by)
        elif isinstance(item, dict):
            return item.get(sort_by, '')
        return ''


class CursorPaginator:
    """Cursor-based pagination for large datasets."""

    def __init__(self, cursor_encode_fn: Callable = None, cursor_decode_fn: Callable = None):
        self.encode = cursor_encode_fn or self._default_encode
        self.decode = cursor_decode_fn or self._default_decode

    def _default_encode(self, data: Dict) -> str:
        """Default cursor encoding."""
        import base64
        import json
        return base64.b64encode(json.dumps(data).encode()).decode()

    def _default_decode(self, cursor: str) -> Dict:
        """Default cursor decoding."""
        import base64
        import json
        try:
            return json.loads(base64.b64decode(cursor.encode()).decode())
        except:
            return {}

    def create_cursor(self, last_item: Any, sort_by: str = "id") -> str:
        """Create a cursor from the last item."""
        if hasattr(last_item, sort_by):
            value = getattr(last_item, sort_by)
        elif isinstance(last_item, dict):
            value = last_item.get(sort_by)
        else:
            value = str(last_item)

        return self.encode({sort_by: value})

    def paginate(self, items: List[Any], cursor: str = None,
                 page_size: int = 20, sort_by: str = "id",
                 sort_order: str = "asc") -> Tuple[List, CursorInfo]:
        """Paginate with cursor."""
        # Decode cursor to get starting point
        start_idx = 0
        if cursor:
            cursor_data = self.decode(cursor)
            start_value = cursor_data.get(sort_by)
            for i, item in enumerate(items):
                item_value = self._get_value(item, sort_by)
                if start_value is not None and item_value >= start_value:
                    start_idx = i + 1
                    break

        # Get page items
        end_idx = min(start_idx + page_size, len(items))
        page_items = items[start_idx:end_idx]

        # Create cursor info
        has_more = end_idx < len(items)
        next_cursor = None
        prev_cursor = None

        if page_items:
            if has_more:
                next_cursor = self.create_cursor(page_items[-1], sort_by)
            if start_idx > 0:
                prev_cursor = self.create_cursor(page_items[0], sort_by)

        cursor_info = CursorInfo(
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
            has_more=has_more
        )

        return page_items, cursor_info

    def _get_value(self, item: Any, key: str):
        """Get value from item."""
        if hasattr(item, key):
            return getattr(item, key)
        elif isinstance(item, dict):
            return item.get(key)
        return None


class KeysetPaginator:
    """
    Keyset pagination (seek method).
    More efficient than offset for large tables.
    """

    def __init__(self, key_columns: List[str]):
        self.key_columns = key_columns  # Columns used for ordering

    def build_seek_query(self, last_row: Dict, query: str) -> str:
        """Build a keyset-based query with seek."""
        conditions = []
        for col in self.key_columns:
            value = last_row.get(col)
            if value is not None:
                conditions.append(f"{col} > '{value}'")

        if conditions:
            where_clause = " AND ".join(conditions)
            return f"{query} WHERE {where_clause}"
        return query

    def create_filter(self, after_key: Dict = None,
                     before_key: Dict = None) -> str:
        """Create filter conditions for keyset pagination."""
        conditions = []

        if after_key:
            for col in self.key_columns:
                value = after_key.get(col)
                if value is not None:
                    conditions.append(f"{col} > {value}")

        if before_key:
            for col in self.key_columns:
                value = before_key.get(col)
                if value is not None:
                    conditions.append(f"{col} < {value}")

        return " AND ".join(conditions) if conditions else "1=1"


class PaginationHelper:
    """
    Helper for adding pagination to any data source.
    """

    def __init__(self):
        self._offset_paginator = OffsetPaginator()
        self._cursor_paginator = CursorPaginator()

    def paginate_list(self, items: List[Any],
                     page: int = 1,
                     page_size: int = 20,
                     sort_by: str = None,
                     sort_order: str = "asc") -> Page:
        """Paginate a Python list."""
        request = self._offset_paginator.create_request(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_order=sort_order
        )
        return self._offset_paginator.paginate(items, request)

    def paginate_with_cursor(self, items: List[Any],
                            cursor: str = None,
                            page_size: int = 20,
                            sort_by: str = "id") -> Tuple[List, CursorInfo]:
        """Paginate with cursor."""
        return self._cursor_paginator.paginate(
            items, cursor, page_size, sort_by
        )

    def create_response(self, page: Page) -> Dict:
        """Create a standardized pagination response."""
        return {
            'items': page.items,
            'pagination': {
                'page': page.page_info.page,
                'page_size': page.page_info.page_size,
                'total_items': page.page_info.total_items,
                'total_pages': page.page_info.total_pages,
                'has_next': page.page_info.has_next,
                'has_prev': page.page_info.has_prev,
                'first_item': page.page_info.first_item,
                'last_item': page.page_info.last_item
            },
            'metadata': page.metadata
        }

    def create_cursor_response(self, items: List,
                               cursor_info: CursorInfo,
                               metadata: Dict = None) -> Dict:
        """Create a cursor-based pagination response."""
        return {
            'items': items,
            'cursor': {
                'next': cursor_info.next_cursor,
                'prev': cursor_info.prev_cursor,
                'has_more': cursor_info.has_more
            },
            'metadata': metadata or {}
        }


class InfiniteScrollPaginator:
    """Paginator for infinite scroll UIs."""

    def __init__(self, page_size: int = 20):
        self.page_size = page_size

    def get_initial(self, items: List[Any]) -> Dict:
        """Get initial page for infinite scroll."""
        page = items[:self.page_size]
        return {
            'items': page,
            'next_key': self._create_next_key(items[self.page_size - 1] if len(items) >= self.page_size else None),
            'has_more': len(items) > self.page_size
        }

    def get_next(self, items: List[Any], next_key: str) -> Dict:
        """Get next page for infinite scroll."""
        # Find starting position based on next_key
        start_idx = 0
        if next_key:
            for i, item in enumerate(items):
                if str(self._get_id(item)) == next_key:
                    start_idx = i + 1
                    break

        end_idx = start_idx + self.page_size
        page = items[start_idx:end_idx]

        return {
            'items': page,
            'next_key': self._create_next_key(items[end_idx - 1] if end_idx < len(items) else None),
            'has_more': end_idx < len(items)
        }

    def _get_id(self, item: Any) -> str:
        """Get ID from item."""
        if hasattr(item, 'id'):
            return str(item.id)
        elif isinstance(item, dict):
            return str(item.get('id', ''))
        return str(item)

    def _create_next_key(self, item: Any) -> Optional[str]:
        """Create next key from item."""
        if item is None:
            return None
        return self._get_id(item)


# Global helper instance
_pagination_helper = PaginationHelper()


def get_pagination_helper() -> PaginationHelper:
    return _pagination_helper


def paginate_list(items: List[Any], **kwargs) -> Page:
    """Paginate a list with defaults."""
    return _pagination_helper.paginate_list(items, **kwargs)


def paginate_cursor(items: List[Any], **kwargs) -> Tuple[List, CursorInfo]:
    """Paginate with cursor."""
    return _pagination_helper.paginate_with_cursor(items, **kwargs)