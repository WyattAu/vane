# Response-headers stage: design proposal
`X-Content-Type-Options`, `Referrer-Policy`, `Server` suppression -
   already an owned, mutable `Vec<u8>` on every branch that injects -
- Per-header conditional logic (only set-if-absent vs overwrite) -