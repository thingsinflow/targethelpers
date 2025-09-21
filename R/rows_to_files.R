# Helper functions for the converting and managing the rows of a data frame as a
# collection of separate files.
# Only the two last functions are exported, the rest are private helper functions.
# By Niels Ole Dam, Sept 2025


##############################################~
####  Functions for saving rows as files  ####
##############################################~

#' Compare two data frames row‑wise by id and flag the changed columns
#'
#' @param df1  First data frame (the “reference” one)
#' @param df2  Second data frame to compare against `df1`
#' @param id   The column name that uniquely identifies a row.
#'             Must exist in both data frames.
#' @return     A copy of `df1` with an additional character column
#'             called `changed_cols`.  Each element contains the names
#'             (comma‑separated) of columns whose values differ between
#'             the two frames.  If no changes are detected the value is
#'             `NA_character_`.
#'
#' @examples
#' \dontrun{
#'   df1 <- data.frame(id = 1:3,
#'                     a   = c(10, 20, 30),
#'                     b   = c("x", "y", "z"),
#'                     stringsAsFactors = FALSE)
#'
#'   df2 <- data.frame(id = 1:3,
#'                     a   = c(10, 25, 30),          # row 2 changed
#'                     b   = c("x", "y", "zz"),      # row 3 changed
#'                     stringsAsFactors = FALSE)
#'
#'   compare_rows(df1, df2)
#'   #   id a  b       changed_cols
#'   # 1  1 10 x                NA
#'   # 2  2 20 y                 a
#'   # 3  3 30 z              b
#' }
compare_rows <- function(df1, df2, id = "id") {
    # Basic sanity checks
    if (!is.data.frame(df1) || !is.data.frame(df2))
        stop("Both inputs must be data frames.")

    if (!(id %in% names(df1)) || !(id %in% names(df2)))
        stop(paste0("The id column '", id, "' is missing in one of the data frames."))

    # Keep only common columns (excluding the id)
    cols_common <- intersect(names(df1), names(df2))
    cols_to_compare <- setdiff(cols_common, id)

    if (length(cols_to_compare) == 0L)
        stop("No columns left to compare after removing '", id, "'.")

    # Merge the two data frames on `id`
    merged <- merge(
        df1[, c(id, cols_to_compare), drop = FALSE],
        df2[, c(id, cols_to_compare), drop = FALSE],
        by = id,
        suffixes = c(".x", ".y")
    )

    # Helper: vectorised comparison that treats NA as a difference
    diff_vec <- function(x, y) {
        # Two values are different if:
        #   - they are not identical, OR
        #   - one is NA and the other is not.
        !identical(x, y)
    }

    # Apply row‑wise
    changed_cols <- apply(merged, 1L, function(row) {
        # Extract values for each column (x vs. y)
        vals_x <- as.list(row[grep("\\.x$", names(row))])
        vals_y <- as.list(row[grep("\\.y$", names(row))])

        diffs <- mapply(diff_vec, vals_x, vals_y)
        if (any(diffs)) {
            paste(cols_to_compare[diffs], collapse = ",")
        } else {
            NA_character_
        }
    })

    # Assemble result
    res <- df1
    res[["changed_cols"]] <- changed_cols

    return(res)
}

#' Compare New or Changed Rows with Existing Files
#'
#' Identifies rows in the input data frame that are new or have changed compared
#' to previous data stored in existing files under `path`. Specific columns that
#' change often (e.g., timestamps) can be specified as such and ignored during
#' comparison.
#'
#' @param new_data_df_w_filepaths A data frame containing the current data and a
#'   `file_path` column that points to where each row should be stored as
#'   separate files.
#' @param cols_not_to_compare Character vector of columns to exclude from
#'   comparison (default is `"images"`).
#' @param id_col_name Name of the unique identifier column (default `"id"`).
#' @param path Directory containing existing files.
#' @param file_prefix Prefix used for the filenames (default `"estate"`).
#' @param extension File extension to match - must be either `".qs2"` or
#'   `".parquet"` (default `".qs2"`).
#'
#' @return A list with two components:
#' \describe{
#'   \item{ids_new_or_changed_rows}{A character vector of IDs that are new or have changed.}
#'   \item{changes}{A data frame}
#'   }
#'
#' @details This is one of three helper functions that work together to keep a
#'   set of per‑property files in sync with the live Boliga dataset:
#'
#'   1. **`compare_with_existing_files()`** Takes the current data frame and
#'   compares every row (ignoring columns that change only because of
#'   timestamps) against the already stored files in the `path` directory.
#'   Returns the IDs of rows that are new or have changed.
#'
#'   2. **`save_each_row_as_a_separate_file()`** Receives the subset of rows
#'   identified as new/changed and writes each of the rows to its own file
#'   (`estate_<id>.qs2` or `estate_<id>.parquet`). The function returns the
#'   paths of the files it created, which can be useful for debugging or
#'   logging.
#'
#'   3. **`delete_existing_files_w_ids_not_present_in_current_data()`** Looks at
#'   all existing files in the directory and removes any whose ID is no longer
#'   present in the current data frame. It returns the list of deleted file
#'   paths.
#'
#'   Finally, the pipeline step returns a vector of the current file paths to
#'   all of the files in the `path` directory, so that `targets` can monitor
#'   these files for downstream steps. This workflow ensures that only
#'   up‑to‑date property records are kept in the input directory and stale ones
#'   are automatically purged.
#'
#'   All of this is done in order for making it possible for `targets` to use
#'   it's branching magic to run the pipeline as efficiently as possible.
#'
#' @seealso These functions are designed to work together, see details above:
#'   [compare_with_existing_files()], [save_each_row_as_a_separate_file()],
#'   [delete_existing_files_w_ids_not_present_in_current_data()]
compare_with_existing_files <- function(new_data_df_w_filepaths,
                                        cols_not_to_compare = c(), # i.e. cols that always contain the current datetime but no other changes etc.
                                        id_col_name = "id",
                                        path,
                                        file_prefix,
                                        extension = ".qs2") {

    # Make sure we're working on a tibble
    new_data_df_w_filepaths <- new_data_df_w_filepaths |> as_tibble()

    # Set read function to use
    read_func <- switch (extension,
                         ".qs2"     = qs_read,
                         ".parquet" = read_parquet,
                         ".rds"     = readRDS,
                         stop)

    # Read data from existing files and compare with new data
    existing_files <- dir(path, paste0("^", file_prefix, "_.+", extension, "$"), full.names = TRUE)
    if (!is_empty(existing_files)) {

        # Only keep the ids/rows that have changed since last run
        old_data <- existing_files |>
            # map_df(readRDS)
            # map_df(qs_read)
            map_df(read_func)
        ids_new_or_changed_rows <- dplyr::setdiff(new_data_df_w_filepaths |>
                                                      select(-"file_path",
                                                             -any_of(cols_not_to_compare)),
                                                  old_data |>
                                                      select(-any_of(cols_not_to_compare)) |>
                                                      unique()) |>
            pull(.data[[id_col_name]])

        # Make summary of changes
        if (length(ids_new_or_changed_rows) > 0) {
            changes <- compare_rows(old_data |> select(-any_of(cols_not_to_compare)) |>
                                        filter(.data[[id_col_name]] %in% ids_new_or_changed_rows),
                                    new_data_df_w_filepaths |>
                                        select(-"file_path",
                                               -any_of(cols_not_to_compare)) |>
                                        filter(.data[[id_col_name]] %in% ids_new_or_changed_rows)
            ) |>
                select(all_of(id_col_name), "changed_cols")

            # If logging in debug mode: output info for each row on which col(s) has/have changed
            if (log_threshold() == as.loglevel("DEBUG")) {
                log_debug("Changed columns:")
                for (n in seq_len(nrow(changes))) {
                    log_debug("{id_col_name}={changes[n, 1]}: {changes[n, 2]}")
                }
            }

            lookup_col_value <- function(data_df, id_col_name, id_to_match, colname) {
                colname <- unlist(strsplit(colname, ","))
                out <- data_df |>
                    filter(.data[[id_col_name]] == id_to_match)
                out[, colname]
            }

            changes <- changes %>%
                mutate(old = purrr::map2_chr(.data[["id"]], .data[["changed_cols"]], ~lookup_col_value(old_data,                id_col_name, .x, .y) |> toJSON()),
                       new = purrr::map2_chr(.data[["id"]], .data[["changed_cols"]], ~lookup_col_value(new_data_df_w_filepaths, id_col_name, .x, .y) |> toJSON()))

            if (nrow(changes) == 0) changes <- NULL
        }
    }

    if (!exists("ids_new_or_changed_rows")) ids_new_or_changed_rows <- new_data_df_w_filepaths[[id_col_name]]
    if (!exists("changes"))                                 changes <- NULL

    # Return a list of ids of all the new or changed rows and summary (compared to existing file data)
    res <- list(ids_new_or_changed_rows = ids_new_or_changed_rows,
                changes                 = changes)
    return(res)
}

#' Save Each Row as a Separate File
#'
#' Writes each row in the input data frame to an individual file, using the
#' `file_path` column for the destination. The data written to disk does not
#' include the `file_path` column itself.
#'
#' @param data_to_save_df_w_filepaths Data frame containing rows to write, with
#'   a `file_path` column specifying the output file for each row.
#' @param id_col_name Name of the unique identifier column (default `"id"`).
#' @param path Directory where files should be saved (default
#'   `"data_in/files"`).
#' @param file_prefix Prefix used for the filenames (default `"estate"`).
#' @param extension File extension to match - must be either `".qs2"` or
#'   `".parquet"` (default `".qs2"`).
#'
#' @return A character vector of the file paths that were created.
#'
#' @details This is one of three helper functions that work together to keep a
#'   set of per‑property files in sync with the live Boliga dataset:
#'
#'   1. **`compare_with_existing_files()`** Takes the current data frame and
#'   compares every row (ignoring columns that change only because of
#'   timestamps) against the already stored files in the `path` directory.
#'   Returns the IDs of rows that are new or have changed.
#'
#'   2. **`save_each_row_as_a_separate_file()`** Receives the subset of rows
#'   identified as new/changed and writes each of the rows to its own file
#'   (`estate_<id>.qs2` or `estate_<id>.parquet`). The function returns the
#'   paths of the files it created, which can be useful for debugging or
#'   logging.
#'
#'   3. **`delete_existing_files_w_ids_not_present_in_current_data()`** Looks at
#'   all existing files in the directory and removes any whose ID is no longer
#'   present in the current data frame. It returns the list of deleted file
#'   paths.
#'
#'   Finally, the pipeline step returns a vector of the current file paths to
#'   all of the files in the `path` directory, so that `targets` can monitor
#'   these files for downstream steps. This workflow ensures that only
#'   up‑to‑date property records are kept in the input directory and stale ones
#'   are automatically purged.
#'
#'   All of this is done in order for making it possible for `targets` to use
#'   it's branching magic to run the pipeline as efficiently as possible.
#'
#' @seealso These functions are designed to work together, see details above:
#'   [compare_with_existing_files()], [save_each_row_as_a_separate_file()],
#'   [delete_existing_files_w_ids_not_present_in_current_data()]
save_each_row_as_a_separate_file <- function(data_to_save_df_w_filepaths,
                                             id_col_name = "id",
                                             path = file.path("data_in", "files"),
                                             file_prefix = "file",
                                             extension = ".qs2") {

    # Set save function to use
    save_func <- switch (extension,
                         ".qs2"     = qs2::qs_save,
                         ".parquet" = write_parquet,
                         ".rds"     = saveRDS,
                         stop)

    # Save an input file for each estate present in the current Boliga unsold dataset that is either new or changed
    data_to_save_df_w_filepaths |>
        group_by(.data[[id_col_name]]) |>
        group_split() |>
        # walk(~saveRDS(. |> select(-file_path), . |> pull(file_path) ))
        walk(~save_func(. |> select(-file_path), . |> pull(file_path) ))

    # Return paths for the new or updated files
    return(data_to_save_df_w_filepaths$file_path)
}

#' Delete Files for Removed Rows
#'
#' Removes files from `path` that correspond to IDs no longer present in the
#' current dataset (`new_data_df_w_filepaths`). File names are derived from the
#' `file_path` column.
#'
#' @param new_data_df_w_filepaths Data frame containing the current data with a
#'   unique `file_path` column value for each row.
#' @param path Directory containing existing files (default
#'   `"data_in/files"`).
#' @param file_prefix Prefix used for the filenames (default `"estate"`).
#' @param extension File extension to match - must be either `".qs2"` or
#'   `".parquet"` (default `".qs2"`).
#'
#' @return A character vector of file paths that were deleted, or NULL if none.
#'
#' @details This is one of three helper functions that work together to keep a
#'   set of per‑property files in sync with the live Boliga dataset:
#'
#'   1. **`compare_with_existing_files()`** Takes the current data frame and
#'   compares every row (ignoring columns that change only because of
#'   timestamps) against the already stored files in the `path` directory.
#'   Returns the IDs of rows that are new or have changed.
#'
#'   2. **`save_each_row_as_a_separate_file()`** Receives the subset of rows
#'   identified as new/changed and writes each of the rows to its own file
#'   (`estate_<id>.qs2` or `estate_<id>.parquet`). The function returns the
#'   paths of the files it created, which can be useful for debugging or
#'   logging.
#'
#'   3. **`delete_existing_files_w_ids_not_present_in_current_data()`** Looks at
#'   all existing files in the directory and removes any whose ID is no longer
#'   present in the current data frame. It returns the list of deleted file
#'   paths.
#'
#'   Finally, the pipeline step returns a vector of the current file paths to
#'   all of the files in the `path` directory, so that `targets` can monitor
#'   these files for downstream steps. This workflow ensures that only
#'   up‑to‑date property records are kept in the input directory and stale ones
#'   are automatically purged.
#'
#'   All of this is done in order for making it possible for `targets` to use
#'   it's branching magic to run the pipeline as efficiently as possible.
#'
#' @seealso These functions are designed to work together, see details above:
#'   [compare_with_existing_files()], [save_each_row_as_a_separate_file()],
#'   [delete_existing_files_w_ids_not_present_in_current_data()]
delete_existing_files_w_ids_not_present_in_current_data <- function(new_data_df_w_filepaths,
                                                                    # id_col_name = "id",
                                                                    path = file.path("data_in", "files"),
                                                                    file_prefix = "file",
                                                                    extension = ".qs2") {
    files_to_delete <- NULL

    # Delete existing files for ids not present in the current dataset
    existing_files <- dir(path, paste0("^", file_prefix, "_.+", extension, "$"))
    if (!is_empty(existing_files)) {
        # new_data_df_w_filepaths <- new_data_df_w_filepaths |>
        #     rows_update(tibble(!!id_col_name := 2111662, propertyType = 39), by = id_col_name) |>
        #     filter(!!rlang::sym(id_col_name) != 2111671)
        new_files <- new_data_df_w_filepaths |>
            mutate(file_name = .data$file_path |> str_remove(fixed(path)) |> str_remove(fixed(.Platform$file.sep))) |>
            pull(.data$file_name)
        # ...delete the files
        files_to_delete <- setdiff(existing_files, new_files)
        if (is_empty(files_to_delete)) files_to_delete <- NULL
        if (!is_null(files_to_delete)) {
            files_to_delete <- file.path(path, files_to_delete)
            files_to_delete |> unlink()
        }
    }

    # Return filepaths of files that was deletes
    return(files_to_delete)
}


#' Add File Paths to a Data Frame
#'
#' Appends a new column `file_path` to the supplied data frame. Each entry is
#' constructed by combining a directory path, an optional file prefix, the value
#' from the identifier column, and a file extension.
#'
#' @param data_df A data.frame or tibble containing rows for which paths are
#'   required.
#' @param id_col_name Character. Name of the column in `data_df` that holds
#'   unique identifiers. Defaults to "id".
#' @param path Character. Directory where the files reside.
#' @param file_prefix Character. String to prepend before the identifier when
#'   forming the file name.
#' @param extension Character. File extension to append; defaults to ".qs2".
#'
#' @return A data frame identical to `data_df` but with an added column
#'   `file_path` containing the constructed file paths.
#' @export
#'
#' @examples
#'   df <- tibble::tibble(id = 1:3)
#'   add_filepaths_to_df(df, id_col_name = "id", path = file.path("data_in", "files"),
#'                       file_prefix = "sample")
add_filepaths_to_df <- function(data_df,
                                id_col_name = "id",
                                path,
                                file_prefix,
                                extension = ".qs2"
) {
    data_df_w_filepaths <- data_df |>
        mutate(file_path = .data[[id_col_name]] %>% map_chr(~file.path(path, paste0(file_prefix, "_", ., extension))))
    return(data_df_w_filepaths)
}


#' Convert the rows of a data frame to individual files
#'
#' Writes each row of a data frame as a separate file, updates changed rows, and
#' removes files for rows that are no longer present. Useful as input for
#' monitoring input files in a Targets pipeline.
#'
#' @details The function creates a directory if needed, compares existing files
#'   to the current data (ignoring specified columns), writes new or updated
#'   rows, deletes obsolete files, and returns a summary of actions taken.
#'
#' @param data_df A data frame containing the rows to be written as separate
#'   files.
#' @param cols_not_to_compare Character vector of column names that should be
#'   ignored when determining if a row has changed (e.g. automatically updated
#'   timestamps).  Defaults to none.
#' @param id_col_name Name of the column that uniquely identifies each row; used
#'   for file naming and comparison. Defaults to "id".
#' @param path Directory where the files will be written; created if it does not
#'   exist. Defaults to "data_in".
#' @param file_prefix Prefix added to each generated file name.  Defaults to
#'   "item".
#' @param extension File extension (including leading dot) used when saving
#'   files. Defaults to ".qs2".
#'
#' @return A list with five components that summarises the actions taken:
#' \describe{
#'   \item{data_df_w_filepaths}{Data frame augmented with the full path of each row’s file.}
#'   \item{ids_new_or_changed_rows}{Vector of ids for rows that are new or have changed.}
#'   \item{changes}{Data frame with summary of what has changed. Each row lists `id`, `changed_cols`, `old`, `new`, with `old` and `new` encoded as JSON.}
#'   \item{new_or_updated_files}{Character vector of paths to files that were created or updated.}
#'   \item{deleted_files}{Character vector of paths to files that were removed because the row is no longer present.}
#' }
#'
#' @export
#'
#' @examples
#' # Example usage
#' df <- data.frame(id = 1:3, value = c("a", "b", "c"))
#' result <- rows_to_files(df, path = "data_out")
#' unlink("data_out", recursive = TRUE)
#'
#' # Typical use case as part of a targets pipeline
#' targets::tar_dir({
#'   targets::tar_script({
#'
#'     library(targets)
#'     library(tarchetypes)
#'
#'     list(
#'       tar_target(
#'         name = data,
#'         command = jsonlite::fromJSON("https://cranlogs.r-pkg.org/top/last-month/100")$downloads
#'       ),
#'       tar_target(
#'         name = rows_as_files,
#'         command = data |>
#'           targethelpers::rows_to_files(id_col_name = "package", extension = ".rds"),
#'         format = "rds"
#'       ),
#'       tar_files(
#'         name = input,
#'         command = rows_as_files$data_df_w_filepaths$file_path
#'       ),
#'       tar_target(
#'         name = whatever,
#'         command = input |>
#'             readRDS()
#'         # Your code here
#'         ,
#'         pattern = map(input)
#'       )
#'     )
#'   })
#'   targets::tar_make()
#'   targets::tar_read(whatever)
#' })
rows_to_files <- function(data_df,                   # The data rows to convert to files
                          cols_not_to_compare = c(), # i.e. cols that always contain the current datetime but no other changes etc.
                          id_col_name         = "id",
                          path                = "data_in",
                          file_prefix         = "item",
                          extension           = ".qs2"
) {

    if (!dir.exists(path)) dir.create(path)

    log_info("Identify rows of new, updated and/or outdated property info + update tracked files.")

    data_df_w_filepaths <- add_filepaths_to_df(data_df,
                                               id_col_name         = id_col_name,
                                               path                = path,
                                               file_prefix         = file_prefix,
                                               extension           = extension)

    # Identify new or changed rows of data
    res <- compare_with_existing_files(data_df_w_filepaths,
                                       cols_not_to_compare = cols_not_to_compare,
                                       id_col_name         = id_col_name,
                                       path                = path,
                                       file_prefix         = file_prefix,
                                       extension           = extension)
    ids_new_or_changed_rows <- res$ids_new_or_changed_rows
    changes                 <- res$changes
    log_info("ids_new_or_changed_rows: {length(ids_new_or_changed_rows)}")
    if (!is_empty(ids_new_or_changed_rows))
        log_debug(ids_new_or_changed_rows |> paste(collapse = ","))


    # Save new or updated rows as separate files
    new_or_updated_files <- save_each_row_as_a_separate_file(data_df_w_filepaths |>
                                                                 filter(.data[[id_col_name]] %in% ids_new_or_changed_rows),
                                                             id_col_name = id_col_name,
                                                             path        = path,
                                                             file_prefix = file_prefix,
                                                             extension   = extension)
    log_info("new_or_updated_files: {length(new_or_updated_files)}")
    if (!is_empty(new_or_updated_files))
        log_debug("{new_or_updated_files}")


    # Delete old files for rows not longer in the data
    deleted_files <- delete_existing_files_w_ids_not_present_in_current_data(data_df_w_filepaths,
                                                                             path        = path,
                                                                             file_prefix = file_prefix,
                                                                             extension   = extension)
    log_info("deleted_files: {length(deleted_files)}")
    if (!is_empty(deleted_files)) log_debug("{deleted_files}")


    # Return summary of results for all the actions above
    summary <- list(data_df_w_filepaths     = data_df_w_filepaths,
                    ids_new_or_changed_rows = ids_new_or_changed_rows,
                    changes                 = changes,
                    new_or_updated_files    = new_or_updated_files,
                    deleted_files           = deleted_files)

    return(summary)
}
