#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include "cozo_c.h"

void run_query(int32_t db_id, const char *query) {
    const char *empty_params = "{}";
    char *res;
    res = cozo_run_query(db_id, query, empty_params);
    printf("%s\n", res);
    cozo_free_str(res);
}

int main() {
    int32_t db_id;
    char *err = cozo_open_db("_test_db", &db_id);

    if (err) {
        printf("%s", err);
        cozo_free_str(err);
        return -1;
    }

    run_query(db_id, "?[] <- [[1, 2, 3]]");

    cozo_close_db(db_id);

    return 0;
}