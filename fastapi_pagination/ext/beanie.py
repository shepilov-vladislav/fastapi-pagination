__all__ = ["paginate"]

from typing import Any, List, Optional, Tuple, Type, TypeVar, Union

from beanie import Document, PydanticObjectId
from beanie.odm.enums import SortDirection
from beanie.odm.interfaces.aggregate import ClientSession, DocumentProjectionType
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.find import FindMany
from bson.errors import InvalidId

from ..api import apply_items_transformer, create_page
from ..bases import AbstractParams, is_cursor, is_limit_offset
from ..types import AdditionalData, AsyncItemsTransformer
from ..utils import verify_params

TDocument = TypeVar("TDocument", bound=Document)


def parse_cursor(cursor: str) -> PydanticObjectId:
    try:
        return PydanticObjectId(cursor.split("_", 1)[-1])
    except InvalidId as exc:
        raise ValueError("Invalid cursor") from exc


async def paginate(  # noqa: C901
    query: Union[TDocument, FindMany[TDocument], AggregationQuery[TDocument]],
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[AsyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
    projection_model: Optional[Type[DocumentProjectionType]] = None,
    sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
    session: Optional[ClientSession] = None,
    ignore_cache: bool = False,
    fetch_links: bool = False,
    lazy_parse: bool = False,
    **pymongo_kwargs: Any,
) -> Any:
    params, raw_params = verify_params(params, "limit-offset", "cursor")
    if additional_data is None:
        additional_data = {}

    cursor = getattr(raw_params, "cursor", None)
    if isinstance(query, AggregationQuery):
        aggregation_query = query.clone()  # type: ignore
        paginate_data = []
        if is_limit_offset(raw_params):
            if raw_params.limit is not None:
                paginate_data.append({"$limit": raw_params.limit + (raw_params.offset or 0)})
            if raw_params.offset is not None:
                paginate_data.append({"$skip": raw_params.offset})
        else:
            if cursor:
                if cursor.startswith("prev_"):
                    paginate_data.append(
                        {
                            "_id": {  # type: ignore[dict-item]
                                "$lt": parse_cursor(cursor),
                            },
                        },
                    )
                else:
                    paginate_data.append(
                        {
                            "_id": {  # type: ignore[dict-item]
                                "$gt": parse_cursor(cursor),
                            },
                        },
                    )

        aggregation_query.aggregation_pipeline.extend(
            [
                {"$facet": {"metadata": [{"$count": "total"}], "data": paginate_data}},
            ],
        )
        data = (await aggregation_query.to_list())[0]
        items = data["data"]
        try:
            total = data["metadata"][0]["total"]
        except IndexError:
            total = 0
        if is_cursor(raw_params):
            if cursor and cursor.startswith("prev_"):
                items = list(reversed(items))
            additional_data["next_"] = str(items[-1].id) if items else None
            additional_data["previous"] = f"prev_{items[0].id}" if items else None
    else:
        if raw_params.include_total:
            total = await query.find(
                {},
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                **pymongo_kwargs,
            ).count()
        else:
            total = None

        if is_limit_offset(raw_params):
            items = await query.find_many(
                limit=raw_params.limit,
                skip=raw_params.offset,
                projection_model=projection_model,
                sort=sort,
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                lazy_parse=lazy_parse,
                **pymongo_kwargs,
            ).to_list()
        else:
            query = query.find_many(
                projection_model=projection_model,  # type: ignore[arg-type]
                sort=sort,
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                lazy_parse=lazy_parse,
                **pymongo_kwargs,
            )
            if cursor:
                if cursor.startswith("prev_"):
                    query = query.find(
                        {
                            "_id": {
                                "$lt": parse_cursor(cursor),
                            },
                        },
                    ).sort("-_id")
                else:
                    query = query.find(
                        {
                            "_id": {
                                "$gt": parse_cursor(cursor),
                            },
                        },
                    )

            items = await query.limit(raw_params.size).to_list()  # type: ignore[attr-defined]
            if cursor and cursor.startswith("prev_"):
                items = list(reversed(items))
            additional_data["next_"] = str(items[-1].id) if items else None
            additional_data["previous"] = f"prev_{items[0].id}" if items else None

    t_items = await apply_items_transformer(items, transformer, async_=True)

    return create_page(
        t_items,
        total=total,
        params=params,
        **(additional_data or {}),
    )
