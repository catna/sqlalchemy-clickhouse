from __future__ import annotations

from infi.clickhouse_orm import comma_join, Database, Field, parse_array
from sqlalchemy.dialects.postgresql import Any


class TupleField(Field):
    class_default: tuple[Any, ...] = ()

    def __init__(self, inner_field: Field, default: Any | None = None,
                 alias: str | None = None, materialized: bool | None = None,
                 readonly: bool | None = None, codec: Any | None = None) -> None:
        assert isinstance(inner_field, Field), (
            'The first argument of TupleField must be a Field instance'
        )
        self.inner_field = inner_field
        super().__init__(default, alias, materialized, readonly, codec)

    def to_python(self, value: Any, timezone_in_use: bool) -> tuple[Any, ...]:
        if isinstance(value, str):
            value = tuple(parse_array(value))
        elif isinstance(value, bytes):
            value = tuple(parse_array(value.decode()))
        elif not isinstance(value, (list, tuple)):
            raise ValueError(
                f'TupleField expects list or tuple, not {type(value)}'
            )
        return tuple(
            self.inner_field.to_python(val, timezone_in_use)
            for val in value
        )

    def validate(self, value: tuple[Any]) -> None:
        for inner_value in value:
            self.inner_field.validate(inner_value)

    def to_db_string(self, value: tuple[Any], quote: bool = True) -> str:
        array = [self.inner_field.to_db_string(v) for v in value]
        return 'tuple(' + comma_join(array) + ')'

    def get_sql(self, with_default_expression: bool = True,
                db: Database | None = None) -> str:
        sql = f'Tuple({self.inner_field.get_sql(with_default_expression=False, db=db)})'
        if with_default_expression and self.codec and db and db.has_codec_support:
            sql += f' CODEC({self.codec})'
        return sql
