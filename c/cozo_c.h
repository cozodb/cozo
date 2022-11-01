#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

extern "C" {

int8_t *cozo_open_db(const int8_t *path, int32_t *db_id);

bool cozo_close_db(int32_t id);

int8_t *cozo_run_query(int32_t db_id,
                       const int8_t *script_raw,
                       const int8_t *params_raw,
                       bool *errored);

void cozo_free_str(int8_t *s);

} // extern "C"
