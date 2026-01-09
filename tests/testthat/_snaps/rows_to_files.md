# errors when inputs are not data frames

    Code
      compare_rows(list(), df1)
    Condition
      Error in `compare_rows()`:
      ! Both inputs must be data frames.

---

    Code
      compare_rows(data.frame(x = 1), "notdf")
    Condition
      Error in `compare_rows()`:
      ! Both inputs must be data frames.

# errors when id column missing in one frame

    Code
      compare_rows(df1, df2)
    Condition
      Error in `compare_rows()`:
      ! The id column 'id' is missing in one of the data frames.

# errors when no columns left to compare after excluding id

    Code
      compare_rows(df1, df2)
    Condition
      Error in `compare_rows()`:
      ! No columns left to compare after removing 'id'.

# generates log message and throws error when new column is present

    Code
      compare_with_existing_files(new_df, cols_not_to_compare = character(), path = temp_path,
      file_prefix = "estate", extension = ".qs2",
      throw_error_if_non_matching_columns = TRUE)
    Condition
      Error in `compare_with_existing_files()`:
      ! Column names for the new and the existing datasets do not match.

# generates log message and throws error when old column is not present

    Code
      compare_with_existing_files(new_df, cols_not_to_compare = character(), path = temp_path,
      file_prefix = "estate", extension = ".qs2",
      throw_error_if_non_matching_columns = TRUE)
    Condition
      Error in `compare_with_existing_files()`:
      ! Column names for the new and the existing datasets do not match.

# errors when id column missing

    Code
      add_filepaths_to_df(df, id_col_name = "id", path = path, file_prefix = "p")
    Condition
      Error in `mutate()`:
      i In argument: `file_path = `%>%`(...)`.
      Caused by error in `.data[["id"]]`:
      ! Column `id` not found in `.data`.

