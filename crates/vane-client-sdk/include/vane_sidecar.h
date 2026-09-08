/* vane_sidecar.h — C ABI for the Vane shared-memory sidecar transport.
 *
 * Link against libvane_shm (static or dynamic). One VaneScClient per
 * thread, or guard calls with your own lock. Payloads are raw bytes;
 * the application defines the request/response encoding.
 */
#ifndef VANE_SIDECAR_H
#define VANE_SIDECAR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque client handle. */
typedef struct VaneScClient VaneScClient;

/* Opens (or attaches to) the transport rooted at base_path.
 * The vane sidecar must already be running.
 * Returns NULL on failure; call vane_sc_last_err() for the reason. */
VaneScClient *vane_sc_open(const char *base_path);

/* Sends `len` bytes. Returns the message id, or UINT64_MAX on error. */
uint64_t vane_sc_send(VaneScClient *client, const uint8_t *payload,
                      size_t len, uint32_t timeout_ms);

/* Receives the next response into `out` (capacity out_len).
 * Returns the payload length, 0 on timeout, -1 on error, -2 if the
 * caller's buffer was too small. Writes the message id to *id_out. */
int vane_sc_recv(VaneScClient *client, uint8_t *out, size_t out_len,
                 uint64_t *id_out, uint32_t timeout_ms);

/* Closes the handle. */
void vane_sc_close(VaneScClient *client);

/* Last error message for this thread (valid until the next call). */
const char *vane_sc_last_err(void);

#ifdef __cplusplus
}
#endif

#endif /* VANE_SIDECAR_H */
