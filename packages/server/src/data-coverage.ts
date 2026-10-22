// Describes the requested range, independently of whether any users matched.
// Absence of retained events cannot prove that an expired interval had no activity.
export function dataCoverage(spec:{from:string;to:string},now=Date.now()){
  const retained=now-30*86400000,from=Date.parse(spec.from),to=Date.parse(spec.to);
  const expired=from<retained,future=to>now;
  return {retained_from:new Date(retained).toISOString(),observed_until:new Date(now).toISOString(),requested_from:spec.from,requested_to:spec.to,expired_overlap:expired,future_overlap:future,incomplete:expired||future};
}
