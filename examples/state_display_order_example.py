"""
Example demonstrating the display_order feature for StateDataColumnDefinition.
This allows columns to maintain a specific order when displayed.
"""

from ismcore.model.processor_state import State, StateConfig, StateDataColumnDefinition


def main():
    # Create a new state
    state = State(
        config=StateConfig(
            name="Display Order Example",
            storage_class="database"
        )
    )
    
    # Add columns without explicit display_order
    # They will be auto-assigned display_order values: 0, 1, 2
    state.add_column(StateDataColumnDefinition(name="name"))
    state.add_column(StateDataColumnDefinition(name="email"))
    state.add_column(StateDataColumnDefinition(name="age"))
    
    print("Initial columns with auto-assigned display_order:")
    for name, col in state.columns.items():
        print(f"  {name}: display_order={col.display_order}")
    
    # Add a column with explicit display_order=10 
    # (higher than existing columns)
    state.add_column(StateDataColumnDefinition(name="priority", display_order=10))
    
    # Add another column without display_order (will get max + 1 = 11)
    state.add_column(StateDataColumnDefinition(name="notes"))
    
    print("\nAfter adding more columns:")
    for name, col in state.columns.items():
        print(f"  {name}: display_order={col.display_order}")
    
    # Get columns sorted by display_order
    sorted_columns = state.get_columns_sorted_by_display_order()
    print("\nColumns sorted by display_order:")
    for col in sorted_columns:
        print(f"  {col.name} (order: {col.display_order})")
    
    # Get column names sorted by display_order
    sorted_names = state.get_column_names_sorted_by_display_order()
    print("\nColumn names in display order:", sorted_names)
    
    # Example with mixed display_order values
    state2 = State(
        config=StateConfig(
            name="Mixed Order Example",
            storage_class="database"
        )
    )
    
    # Add columns with specific display orders
    state2.add_column(StateDataColumnDefinition(name="col_c", display_order=2))
    state2.add_column(StateDataColumnDefinition(name="col_a", display_order=0))
    state2.add_column(StateDataColumnDefinition(name="col_b", display_order=1))
    state2.add_column(StateDataColumnDefinition(name="col_d"))  # Will get display_order=3
    
    print("\n\nMixed order example:")
    sorted_names2 = state2.get_column_names_sorted_by_display_order()
    print("Column names in display order:", sorted_names2)


if __name__ == "__main__":
    main()