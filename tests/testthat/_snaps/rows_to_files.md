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

# errors when id column missing

    Code
      add_filepaths_to_df(df, id_col_name = "id", path = "/tmp", file_prefix = "p")
    Condition
      Error in `mutate()`:
      i In argument: `file_path = `%>%`(...)`.
      Caused by error in `.data[["id"]]`:
      ! Column `id` not found in `.data`.

