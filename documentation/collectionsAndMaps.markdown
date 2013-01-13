## Collections and Maps support

 **Achilles** support mapping for Set, List and Map types. The underlying implementation returned by a *managed*
 entity is HashSet, ArrayList and HashMap.
 
 All collections and maps fields are eagerly fetched by default when the entity is loaded. However you can make
 them lazy with the  *@Lazy* annotation.

 Even if set as *lazy*, collection or map are loaded entirely in memory. Therefore we strongly advise to limit
 the use of collections and maps for **small number of items (<1000)**.
 
 If you need to store more than 1000 items, consider using **internal** or **external wide rows**. Check [Internal wide row][internalWideRow]
 and [External wide row][externalWideRow] for more details

[internalWideRow]: /doanduyhai/achilles/tree/master/documentation/internalWideRow.markdown
[externalWideRow]: /doanduyhai/achilles/tree/master/documentation/externalWideRow.markdown 
 
 
 
 