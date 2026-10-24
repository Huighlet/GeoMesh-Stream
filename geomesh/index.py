"""
GeoMesh-Stream: Discrete Spatial Grid Index.
Discretizes coordinates into uniform spatial cells for O(1) candidate lookup and AABB pruning.
"""

from __future__ import annotations

import collections
import math
from typing import Dict, List, Optional, Set, Tuple

from geomesh.types import BoundingBox, Point


class SpatialGridIndex:
    """
    Uniform Spatial Grid Hash Index.
    Divides the globe into uniform lat/lon cells of resolution cell_size_deg.
    Maps cell keys (row, col) to collections of indexed entities or geofence identifiers.
    """

    def __init__(self, cell_size_deg: float = 0.02) -> None:
        if cell_size_deg <= 0.0:
            raise ValueError(f"cell_size_deg must be positive, got {cell_size_deg}")
        self.cell_size = cell_size_deg
        # (row, col) -> Set[identifier]
        self._grid: Dict[Tuple[int, int], Set[str]] = collections.defaultdict(set)
        # identifier -> BoundingBox (for fast removal)
        self._item_bboxes: Dict[str, BoundingBox] = {}

    def _coord_to_cell(self, lat: float, lon: float) -> Tuple[int, int]:
        row = math.floor(lat / self.cell_size)
        col = math.floor(lon / self.cell_size)
        return (row, col)

    def _bbox_to_cells(self, bbox: BoundingBox) -> List[Tuple[int, int]]:
        min_row = math.floor(bbox.min_lat / self.cell_size)
        max_row = math.floor(bbox.max_lat / self.cell_size)
        min_col = math.floor(bbox.min_lon / self.cell_size)
        max_col = math.floor(bbox.max_lon / self.cell_size)

        cells = []
        for r in range(min_row, max_row + 1):
            for c in range(min_col, max_col + 1):
                cells.append((r, c))
        return cells

    def insert(self, item_id: str, bbox: BoundingBox) -> None:
        """Insert an item into all grid cells spanned by its bounding box."""
        if item_id in self._item_bboxes:
            self.remove(item_id)

        self._item_bboxes[item_id] = bbox
        for cell in self._bbox_to_cells(bbox):
            self._grid[cell].add(item_id)

    def remove(self, item_id: str) -> None:
        """Remove an item from the spatial grid."""
        bbox = self._item_bboxes.pop(item_id, None)
        if not bbox:
            return

        for cell in self._bbox_to_cells(bbox):
            if cell in self._grid:
                self._grid[cell].discard(item_id)
                if not self._grid[cell]:
                    del self._grid[cell]

    def query_point(self, point: Point) -> Set[str]:
        """Return all item IDs whose bounding boxes span the cell containing the point."""
        cell = self._coord_to_cell(point.lat, point.lon)
        return set(self._grid.get(cell, set()))

    def query_radius(self, center: Point, radius_m: float) -> Set[str]:
        """Return all item IDs whose bounding boxes intersect the circle's bounding box."""
        # Approximate 1 degree latitude ~ 111,320 meters
        d_lat = radius_m / 111_320.0
        cos_lat = math.cos(math.radians(center.lat))
        d_lon = radius_m / (111_320.0 * max(0.01, cos_lat))

        search_bbox = BoundingBox(
            min_lat=max(-90.0, center.lat - d_lat),
            min_lon=max(-180.0, center.lon - d_lon),
            max_lat=min(90.0, center.lat + d_lat),
            max_lon=min(180.0, center.lon + d_lon),
        )

        candidates: Set[str] = set()
        for cell in self._bbox_to_cells(search_bbox):
            if cell in self._grid:
                candidates.update(self._grid[cell])

        return candidates

    def clear(self) -> None:
        """Clear all indexed cells."""
        self._grid.clear()
        self._item_bboxes.clear()

    @property
    def total_cells(self) -> int:
        return len(self._grid)

    @property
    def total_items(self) -> int:
        return len(self._item_bboxes)
