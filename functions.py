import pandas as pd
def replace_yes_no(df, columns):
    """
    Replace non-missing values with 'Yes' and missing values with 'No' 
    for the specified columns in a pandas DataFrame.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame.
    columns (list): List of column names to process.
    
    Returns:
    pd.DataFrame: Updated DataFrame with 'Yes'/'No' values.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: 'Yes' if pd.notna(x) else 'No')
    return df

def replace_values(df: pd.DataFrame, columns: list, replacements: dict) -> pd.DataFrame:
    """
    Replace values in specified DataFrame columns based on a replacements dictionary.
    
    Parameters:
    ----------
    df : pd.DataFrame
        The input DataFrame.
    columns : list
        List of column names in which to perform replacements.
    replacements : dict
        Dictionary specifying old values as keys and new values as values.
        Example: {"old_value1": "new_value1", "old_value2": "new_value2"}
        
    Returns:
    -------
    pd.DataFrame
        A new DataFrame with the replaced values.
    """
    df_copy = df.copy()
    for col in columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].replace(replacements)
        else:
            print(f"Warning: Column '{col}' not found in DataFrame.")
    return df_copy

def merge_columns(df, column_names, new_column_name):
    """
    Merges values from specified columns into a new column.
    The first non-null/non-empty value encountered in each row is used.
    
    Parameters:
    df (pd.DataFrame): The dataframe containing the columns.
    column_names (list): List of column names to check.
    new_column_name (str): The name of the new column.
    
    Returns:
    pd.DataFrame: DataFrame with the new column added.
    """
    df[new_column_name] = df[column_names].bfill(axis=1).iloc[:, 0]
    return df