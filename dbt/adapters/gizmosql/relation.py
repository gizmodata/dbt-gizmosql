from dbt.adapters.base.relation import BaseRelation, Policy

class GizmoSQLIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


class GizmoSQLQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


class GizmoSQLRelation(BaseRelation):
    include_policy = GizmoSQLIncludePolicy()
    quote_policy = GizmoSQLQuotePolicy()
