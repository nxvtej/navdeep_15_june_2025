from app.database.db import engine
from sqlalchemy.dialects.postgresql import insert


# Conflict handler
def _get_insert_statement_on_conflict(table, batch, conflict_index: list):
    dialect_name = engine.dialect.name
    # print(f"Using dialect: {dialect_name} for conflict handling.")
    # exit(1)
    stmt = insert(table).values(batch)
    
    if dialect_name == 'postgresql':
        return stmt.on_conflict_do_nothing(index_elements=conflict_index)
    elif dialect_name == 'mysql':
        return stmt.prefix_with('IGNORE')
    elif dialect_name == 'sqlite':
        return stmt.on_conflict_do_nothing(index_elements=conflict_index)
    else:
        print(f"Warning: Using generic INSERT for dialect {dialect_name}. "
              f"ON CONFLICT DO NOTHING may not be supported or mapped correctly for bulk operations.")
        return stmt
