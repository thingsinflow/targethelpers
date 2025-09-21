# compare_rows -----------------------------------------------------------------

test_that("returns correct changed_cols for example", {
    df1 <- data.frame(id = 1:3,
                      a   = c(10, 20, 30),
                      b   = c("x", "y", "z"),
                      stringsAsFactors = FALSE)
    df2 <- data.frame(id = 1:3,
                      a   = c(10, 25, 30),
                      b   = c("x", "y", "zz"),
                      stringsAsFactors = FALSE)
    res <- compare_rows(df1, df2)
    expect_equal(res$changed_cols, c(NA_character_, "a", "b"))
})

test_that("handles rows with no changes", {
    df1 <- data.frame(id = 1:2, x = 1:2, stringsAsFactors = FALSE)
    df2 <- data.frame(id = 1:2, x = 1:2, stringsAsFactors = FALSE)
    res <- compare_rows(df1, df2)
    expect_equal(res$changed_cols, c(NA_character_, NA_character_))
})

test_that("flags multiple changed columns per row", {
    df1 <- data.frame(id = 1,
                      a = 10, b = "x", stringsAsFactors = FALSE)
    df2 <- data.frame(id = 1,
                      a = 20, b = "y", stringsAsFactors = FALSE)
    res <- compare_rows(df1, df2)
    expect_equal(res$changed_cols, "a,b")
})

test_that("treats NA vs non-NA as difference", {
    df1 <- data.frame(id = 1:2,
                      a = c(NA, 5),
                      stringsAsFactors = FALSE)
    df2 <- data.frame(id = 1:2,
                      a = c(10, NA),
                      stringsAsFactors = FALSE)
    res <- compare_rows(df1, df2)
    expect_equal(res$changed_cols, c("a", "a"))
})

test_that("errors when inputs are not data frames", {
    expect_snapshot(compare_rows(list(), df1), error = TRUE)
    expect_snapshot(compare_rows(data.frame(x=1), "notdf"), error = TRUE)
})

test_that("errors when id column missing in one frame", {
    df1 <- data.frame(id = 1:2, a = 1:2, stringsAsFactors = FALSE)
    df2 <- data.frame(other_id = 1:2, a = 1:2, stringsAsFactors = FALSE)
    expect_snapshot(compare_rows(df1, df2), error = TRUE)
})

test_that("errors when no columns left to compare after excluding id", {
    df1 <- data.frame(id = 1:2, stringsAsFactors = FALSE)
    df2 <- data.frame(id = 1:2, stringsAsFactors = FALSE)
    expect_snapshot(compare_rows(df1, df2), error = TRUE)
})

test_that("works when custom id column is used", {
    df1 <- data.frame(another_id = 1:3,
                      a = c(10,20,30),
                      b = c("x","y","z"),
                      stringsAsFactors = FALSE)
    df2 <- data.frame(another_id = 1:3,
                      a = c(10,25,30),
                      b = c("x","y","zz"),
                      stringsAsFactors = FALSE)
    res <- compare_rows(df1, df2, id = "another_id")
    expect_equal(res$changed_cols, c(NA_character_, "a", "b"))
})


# compare_with_existing_files --------------------------------------------------

# TODO: duplicate all tests to validate for when extention = ".parquet" and ".rds" also ?

test_that("returns all IDs when no existing files present", {
    withr::local_tempdir()
    test_data <- data.frame(
        id = c(1L, 2L, 3L),
        name = c("A", "B", "C"),
        file_path = c("path1", "path2", "path3")
    )
    result <- compare_with_existing_files(test_data, path = "nonexistent_dir", file_prefix = "test")
    # Verify `ids_new_or_changed_rows`
    res <- result$ids_new_or_changed_rows
    expect_equal(res, c(1, 2, 3))
    # Verify `changes`
    res <- result$changes
    expect_null(res)
})

test_that("detects new and changed rows", {
    temp_path <- withr::local_tempdir()
    qs2::qs_save(tibble::tibble(id = 1L, val = "a"), file.path(temp_path,"estate_1.qs2"))
    qs2::qs_save(tibble::tibble(id = 2L, val = "b"), file.path(temp_path,"estate_2.qs2"))
    new_df <- tibble::tibble(id = c(1L,2L), val = c("a","c") , file_path = c("estate_1.qs2", "estate_2.qs2"))
    result <- compare_with_existing_files(new_df, path = temp_path, file_prefix = "estate")
    # Verify `ids_new_or_changed_rows`
    res <- result$ids_new_or_changed_rows
    expect_equal(res, 2L)
    # Verify `changes`
    res <- result$changes
    old <- res$old |> fromJSON()
    new <- res$new |> fromJSON()
    expect_equal(old, data.frame(val = "b"))
    expect_equal(new, data.frame(val = "c"))
})

test_that("returns summary of all changes when multiple columns have changed", {
    temp_path <- withr::local_tempdir()
    qs2::qs_save(tibble::tibble(id = 1L, val = "a", val2 = 1, val3 = "x"), file.path(temp_path,"estate_1.qs2"))
    qs2::qs_save(tibble::tibble(id = 2L, val = "b", val2 = 1, val3 = "x"), file.path(temp_path,"estate_2.qs2"))
    new_df <- tibble::tibble(id = c(1L,2L), val = c("a","c"), val2 = c(1, 2), val3 = c("x", "x"), file_path = c("estate_1.qs2", "estate_2.qs2"))
    result <- compare_with_existing_files(new_df, path = temp_path, file_prefix = "estate")
    # Verify `ids_new_or_changed_rows`
    res <- result$ids_new_or_changed_rows
    expect_equal(res, 2L)
    # Verify `changes`
    res <- result$changes
    old <- res$old |> fromJSON()
    new <- res$new |> fromJSON()
    expect_equal(res$changed_cols, "val,val2")
    expect_equal(old, data.frame(val = "b", val2 = 1))
    expect_equal(new, data.frame(val = "c", val2 = 2))
})

test_that("ignores specified columns in comparison", {
    temp_path <- withr::local_tempdir()
    qs2::qs_save(tibble::tibble(id = 1L, val = "a", xyz = "old"), file.path(temp_path,"estate_1.qs2"))
    qs2::qs_save(tibble::tibble(id = 2L, val = "b", xyz = "old"), file.path(temp_path,"estate_2.qs2"))
    new_df <- tibble::tibble(id = c(1L,2L), val = c("a","c"), xyz = c("new", "new") , file_path = c("estate_1.qs2", "estate_2.qs2"))
    result <- compare_with_existing_files(new_df, cols_not_to_compare = c("xyz"), path = temp_path, file_prefix = "estate")
    # Verify `ids_new_or_changed_rows`
    res <- result$ids_new_or_changed_rows
    expect_equal(res, 2L)
    # Verify `changes`
    res <- result$changes
    old <- res$old |> fromJSON()
    new <- res$new |> fromJSON()
    expect_equal(old, data.frame(val = "b"))
    expect_equal(new, data.frame(val = "c"))
})

test_that("no debug logging when no changes or not in debug mode", {
    temp_dir <- tempfile()
    dir.create(temp_dir)
    file_path <- file.path(temp_dir, "estate_2.qs2")
    writeLines("", con = file_path) # dummy file
    new_df <- tibble::tibble(id = 2, name = "new", file_path = file_path)

    log_calls <- list()
    testthat::local_mocked_bindings(
        map_df = function(x, ...) data.frame(id = 3, name = "old"),
        compare_rows = function(old, new) {
            tibble::tibble(id = integer(0), changed_cols = character(0))
        },
        log_threshold = function() as.loglevel("INFO"),
        as.loglevel = function(x) x,
        log_debug = function(...) log_calls <<- c(log_calls, list(list(...)))
    )

    result <- compare_with_existing_files(new_df, path = temp_dir, file_prefix = "estate", extension = ".qs2")
    # Verify `ids_new_or_changed_rows`
    ids <- result$ids_new_or_changed_rows
    expect_equal(ids, 2)
    expect_length(log_calls, 0)
    # Verify `changes`
    res <- result$changes
    expect_null(res)
})

test_that("debug logging when changes and in debug mode", {
    temp_path <- withr::local_tempdir()
    qs2::qs_save(tibble::tibble(id = 1L, val = "a"), file.path(temp_path,"estate_1.qs2"))
    qs2::qs_save(tibble::tibble(id = 2L, val = "b"), file.path(temp_path,"estate_2.qs2"))
    new_df <- tibble::tibble(id = c(1L,2L), val = c("a","c") , file_path = c("estate_1.qs2", "estate_2.qs2"))

    log_calls <- list()
    testthat::local_mocked_bindings(
        log_threshold = function() as.loglevel("DEBUG"),
        log_debug = function(...) log_calls <<- c(log_calls, ...) |> paste(collapse = "\n")
    )

    result <- compare_with_existing_files(new_df, path = temp_path, file_prefix = "estate")
    # Verify `ids_new_or_changed_rows`
    ids <- result$ids_new_or_changed_rows
    expect_equal(ids, 2)
    expect_match(log_calls, "\\{id_col_name\\}")
    # Verify `changes`
    res <- result$changes
    old <- res$old |> fromJSON()
    new <- res$new |> fromJSON()
    expect_equal(old, data.frame(val = "b"))
    expect_equal(new, data.frame(val = "c"))
})


# save_each_row_as_a_separate_file ---------------------------------------------

# TODO: duplicate all tests to validate for when extention = ".parquet" and ".rds" also ?

test_that("writes each row to its own file without the file_path", {
    temp_dir <- withr::local_tempdir()
    df <- tibble::tibble(
        id = c(1, 2),
        value = c("a", "b"),
        file_path = file.path(temp_dir, paste0("estate_", 1:2, ".qs2"))
    )
    res_paths <- save_each_row_as_a_separate_file(df, path = temp_dir)
    expect_equal(res_paths, df$file_path)

    for (i in seq_len(nrow(df))) {
        expect_true(file.exists(df$file_path[i]))
        loaded <- qs2::qs_read(df$file_path[i])
        expect_named(loaded, setdiff(names(df), "file_path"))
        expect_equal(loaded$id, df$id[i])
        expect_equal(loaded$value, df$value[i])
    }
})


# delete_existing_files_w_ids_not_present_in_current_data ----------------------

# TODO: duplicate all tests to validate for when extention = ".parquet" and ".rds" also ?

test_that("deletes files whose IDs are not in the current dataset", {
    temp_path <- withr::local_tempdir()
    # create existing files
    file1 <- file.path(temp_path, "estate_1.qs2")
    file2 <- file.path(temp_path, "estate_2.qs2")
    file3 <- file.path(temp_path, "estate_3.qs2")
    writeLines("dummy", file1)
    writeLines("dummy", file2)
    writeLines("dummy", file3)

    # current dataset only has id 2
    new_data_df_w_filepaths <- tibble::tibble(
        file_path = c(file.path(temp_path, "estate_2.qs2"))
    )

    deleted <- delete_existing_files_w_ids_not_present_in_current_data(new_data_df_w_filepaths,
                                                                       path = temp_path,
                                                                       file_prefix = "estate",
                                                                       extension = ".qs2")
    expect_equal(sort(deleted), sort(c(file1, file3)))
    expect_false(file.exists(file1))
    expect_true(file.exists(file2))
    expect_false(file.exists(file3))
})

test_that("returns NULL when no files need to be deleted", {
    temp_path <- withr::local_tempdir()
    # create one existing file
    file1 <- file.path(temp_path, "estate_10.qs2")
    writeLines("dummy", file1)

    new_data_df_w_filepaths <- tibble::tibble(
        file_path = c(file.path(temp_path, "estate_10.qs2"))
    )

    deleted <- delete_existing_files_w_ids_not_present_in_current_data(new_data_df_w_filepaths,
                                                                       path = temp_path,
                                                                       file_prefix = "estate",
                                                                       extension = ".qs2")
    expect_null(deleted)
    expect_true(file.exists(file1))
})

test_that("handles empty directory gracefully", {
    temp_path <- withr::local_tempdir()
    new_data_df_w_filepaths <- tibble::tibble(
        file_path = character(0)
    )

    deleted <- delete_existing_files_w_ids_not_present_in_current_data(new_data_df_w_filepaths,
                                                                       path = temp_path,
                                                                       file_prefix = "estate",
                                                                       extension = ".qs2")
    expect_null(deleted)
})


# add_filepaths_to_df ----------------------------------------------------------

test_that("adds file_path column with correct paths", {
    df <- tibble::tibble(id = c(10, 20))
    path <- file.path("data", "files")
    res <- add_filepaths_to_df(df, id_col_name = "id",
                               path = path,
                               file_prefix = "sample")
    expect_true("file_path" %in% names(res))
    expect_equal(res$file_path,
                 c(file.path(path, "sample_10.qs2"),
                   file.path(path, "sample_20.qs2")))
})

test_that("uses default extension when none supplied", {
    df <- tibble::tibble(id = 1)
    path <- file.path("tmp")
    res <- add_filepaths_to_df(df, id_col_name = "id",
                               path = path,
                               file_prefix = "x")
    expect_equal(res$file_path,
                 file.path(path, "x_1.qs2"))
})

test_that("errors when id column missing", {
    df <- tibble::tibble(other = 1:3)
    path <- file.path("tmp")
    expect_snapshot(
        add_filepaths_to_df(df, id_col_name = "id",
                            path = path,
                            file_prefix = "p"),
        error = TRUE
    )
})


# rows_as_files ----------------------------------------------------------------

test_that("creates directory when missing", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    tmp <- withr::local_tempdir()
    dir_that_should_be_automatically_created <- file.path(tmp, "data_in")
    on.exit(unlink(tmp, recursive = TRUE), add = TRUE)
    expect_false(dir.exists(dir_that_should_be_automatically_created))
    rows_as_files(data_df = data.frame(id = 1, x = "a"),
                  path = dir_that_should_be_automatically_created)
    expect_true(dir.exists(dir_that_should_be_automatically_created))
})

test_that("writes all rows as new files", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    tmp <- withr::local_tempdir()
    df <- data.frame(id = 1:2, value = c("a", "b"))
    res <- rows_as_files(df, path = tmp)
    expect_equal(length(res$new_or_updated_files), 2L)
    expect_true(all(file.exists(res$new_or_updated_files)))
    expect_identical(res$deleted_files, NULL)
    expect_true("file_path" %in% colnames(res$data_df_w_filepaths))
    expect_equal(sort(res$data_df_w_filepaths$file_path), sort(res$new_or_updated_files))
    # Verify `changes`
    expect_null(res$changes)
})

test_that("updates and deletes files when data changes", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    tmp <- withr::local_tempdir()
    df1 <- data.frame(id = 1:2, value = c("a", "b"))
    res1 <- rows_as_files(df1, path = tmp)
    expect_true(all(file.exists(res1$new_or_updated_files)))
    # keep track of file for id 2
    file_id2 <- res1$data_df_w_filepaths$file_path[res1$data_df_w_filepaths$id == 2]
    df2 <- data.frame(id = c(1, 3), value = c("a", "c"))
    res2 <- rows_as_files(df2, path = tmp)
    # id 2 file should be deleted
    expect_true(file_id2 %in% res2$deleted_files)
    expect_false(file.exists(file_id2))
    # new file for id 3 should exist
    file_id3 <- res2$data_df_w_filepaths$file_path[res2$data_df_w_filepaths$id == 3]
    expect_true(file.exists(file_id3))
    # only two files remain in directory
    expect_equal(length(list.files(tmp)), 2L)
    # Verify `changes`
    expect_null(res2$changes)
})

test_that("creates files correctly with default parameters", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    df <- data.frame(id = 1:3, value = letters[1:3])
    tmp_path <- withr::local_tempdir()
    res <- rows_as_files(df, path = tmp_path)
    expect_true(dir.exists(tmp_path))
    expect_equal(nrow(res$data_df_w_filepaths), nrow(df))
    expect_true(all(file.exists(res$new_or_updated_files)))
    expect_false(length(res$deleted_files) > 0)
    expect_identical(names(res$data_df_w_filepaths)[ncol(res$data_df_w_filepaths)], "file_path")
    expect_equal(sort(basename(res$new_or_updated_files)),
                 paste0("item_", df$id, ".qs2"))
})

test_that("updates changed rows and ignores cols_not_to_compare", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    df <- data.frame(id = 1:2, value = c("a","b"), ts = Sys.time())
    tmp_path <- withr::local_tempdir()
    res1 <- rows_as_files(df, path = tmp_path)
    # change value in row 2 but keep ts unchanged
    df$value[2] <- "c"
    res2 <- rows_as_files(df, cols_not_to_compare = "ts", id_col_name = "id",
                          path = tmp_path)
    expect_equal(length(res2$ids_new_or_changed_rows), 1L)
    expect_true(file.exists(res2$new_or_updated_files))
    expect_false(length(res2$deleted_files) > 0)
    # Verify `changes`
    res <- res2$changes
    old <- res$old |> fromJSON()
    new <- res$new |> fromJSON()
    expect_equal(old, data.frame(value = "b"))
    expect_equal(new, data.frame(value = "c"))
    # ts changed but ignored
    df$ts[2] <- Sys.time()
    res3 <- rows_as_files(df, cols_not_to_compare = "ts", id_col_name = "id",
                          path = tmp_path)
    expect_equal(length(res3$ids_new_or_changed_rows), 0L)  # no row changed (because `ts` is ignored)
})

test_that("deletes files for removed rows", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    df <- data.frame(id = 1:3, value = letters[1:3])
    tmp_path <- withr::local_tempdir()
    rows_as_files(df, path = tmp_path)
    df_new <- df[-3, ]   # remove id 3
    res_del <- rows_as_files(df_new, path = tmp_path)
    expect_equal(length(res_del$deleted_files), 1L)
    expect_true(file.exists(file.path(tmp_path,"item_3.qs2")) == FALSE)
})

test_that("supports custom id_col_name and file_prefix", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    df <- data.frame(pkg = c("A","B"), val = 1:2)
    tmp_path <- withr::local_tempdir()
    res <- rows_as_files(df, id_col_name = "pkg", file_prefix = "proj",
                         path = tmp_path)
    expect_equal(sort(basename(res$new_or_updated_files)),
                 paste0("proj_", df$pkg, ".qs2"))
})

test_that("writes files with different extensions", {
    testthat::local_mocked_bindings(
        log_info = function(...) NULL
    )
    exts <- c(".qs2", ".parquet", ".rds")
    for (ext in exts) {
        df <- data.frame(id = 1:2, value = letters[1:2])
        tmp_path <- withr::local_tempdir()
        res <- rows_as_files(df, extension = ext, path = tmp_path)
        expect_equal(sort(basename(res$new_or_updated_files)),
                     paste0("item_", df$id, ext))
    }
})
