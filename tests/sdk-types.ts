import type{TypedAnalytics}from'../packages/sdk-browser/src/index';
import type{TypedNodeClient}from'../packages/sdk-node/src/index';
type BusinessEvents={purchase_completed:{order_id:string;amount:number};help_clicked:{}};
function check(client:TypedAnalytics<BusinessEvents>){
  client.track('purchase_completed',{order_id:'o-1',amount:10});client.track('help_clicked');
  // @ts-expect-error Unknown business event names must not compile.
  client.track('unknown_event',{});
  // @ts-expect-error Required business attributes must not be optional.
  client.track('purchase_completed');
  // @ts-expect-error Numeric attributes must remain numeric.
  client.track('purchase_completed',{order_id:'o-1',amount:'10'});
}
void check;
function nodeCheck(client:TypedNodeClient<BusinessEvents>){
  client.trackFor('user-A','purchase_completed',{order_id:'o-1',amount:10});client.trackFor('user-B','help_clicked');
  // @ts-expect-error Per-event identity does not erase required attributes.
  client.trackFor('user-A','purchase_completed');
  // @ts-expect-error Per-event identity does not erase event names.
  client.trackFor('user-A','unknown_event',{});
  // @ts-expect-error Per-event identity does not erase property types.
  client.trackFor('user-A','purchase_completed',{order_id:'o-1',amount:'10'});
}
void nodeCheck;

function timeOptions(browser:TypedAnalytics<BusinessEvents>,node:TypedNodeClient<BusinessEvents>){
  browser.track('purchase_completed',{order_id:'o',amount:1},{event_time:'2026-10-23T00:00:00.000Z'});
  browser.track('help_clicked',undefined,{event_time:'2026-10-23T00:00:00.000Z'});
  node.trackFor('A','purchase_completed',{order_id:'o',amount:1},{event_time:'2026-10-23T00:00:00.000Z'});
  // @ts-expect-error Business time must be a string, not a Date object.
  browser.track('help_clicked',{}, {event_time:new Date()});
  // @ts-expect-error Options must not allow skipping required business properties.
  node.trackFor('A','purchase_completed',undefined,{event_time:'2026-10-23T00:00:00.000Z'});
}
void timeOptions;
