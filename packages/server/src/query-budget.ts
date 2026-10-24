// Single-process preview budget shared by every synchronous analytics endpoint.
// Multi-instance production requires a coordinated budget, not independent per-process sets.
export const projectQueryFlights=new Set<string>();
