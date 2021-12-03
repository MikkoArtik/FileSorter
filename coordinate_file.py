from typing import NamedTuple, List, Dict, Tuple
import csv


class Coordinate(NamedTuple):
    point_name: str
    x: float
    y: float


class CoordinatesFile:
    def __init__(self, path: str, name_column: int, x_wgs84_column: int,
                 y_wgs84_column: int, skip_rows: int):
        self.__path = path
        self.name_column = name_column
        self.x_wgs84_column = x_wgs84_column
        self.y_wgs84_column = y_wgs84_column
        self.skip_rows = skip_rows

    @property
    def path(self) -> str:
        return self.__path

    @property
    def coordinates(self) -> List[Coordinate]:
        coords_list = []
        with open(self.path) as file_ctx:
            reader = csv.reader(file_ctx)
            for _ in range(self.skip_rows):
                next(reader)

            for row in reader:
                if not len(row):
                    continue
                name = row[self.name_column]
                x_wgs84 = float(row[self.x_wgs84_column])
                y_wgs84 = float(row[self.y_wgs84_column])
                coords_list.append(Coordinate(name, x_wgs84, y_wgs84))
        return coords_list

    @property
    def coordinates_as_dict(self) -> Dict[str, Tuple[float, float]]:
        dict_coords = {}
        for coord in self.coordinates:
            dict_coords[coord.point_name] = (coord.x, coord.y)
        return dict_coords

