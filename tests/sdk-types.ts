import type{TypedAnalytics}from'../packages/sdk-browser/src/index';
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
