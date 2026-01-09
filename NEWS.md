# targethelpers 0.3.1

-   Reduced log level from error to warn for internal function compare_with_existing_files() in case of colnames mismatch.

# targethelpers 0.3.0

-   Added a setting (throw_error_if_non_matching_columns) to the compare_with_existing_files() internal function.
    -   If TRUE the function throws an error, if the datasets to compare do not have identical colnames. If FALSE (the default), columns with empty value and correct type (=same type as in the other dataset) is inserted before the comparison.

# targethelpers 0.2.3

-   Moved the informative error message to earlier in the code, as it was placed to late.

# targethelpers 0.2.2

-   rows_as_files(): Added informative log error message and throws a error if existing and new datasets to compare do not match.

# targethelpers 0.2.1

-   Fixed an error during comparison of datasets with id_col_name other than "id".

# targethelpers 0.2.0

# targethelpers 0.1.0

-   Initial CRAN submission.
