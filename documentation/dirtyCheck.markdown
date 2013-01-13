## Dirty check

 Dirty check is performed only on *managed* entities and on all fields that are not of **WideMap** type. It includes
 of course collections and map fields.
 
 Internally, **Achilles** intercepts all setter invocations upon mapped fields of *managed* entities and flag them as
 dirty. At flush time , when calling merge(), all dirty fields are persisted in **Cassandra**. This is done by using
 batch insert with **Hector** mutator. All the changes will be sent at once to **Cassandra**. 
 
 However, since there is a default limit of 15Mb in a single message payload (*thrift_transport_size_in_mb* property
 in **cassandra.yaml**), we strongly recommend not to have too many elements in dirty collections/maps or too big POJO 
 as field.
 
 **Achilles** extends collections and maps dirty check support to the iterator, entrySet, entryIterator, keySet, 
 listIterator and valueCollections returned by them.
 
 Below is a matrix of types and operations supported by dirty check:
 
<table border=0 cellpadding=0 cellspacing=0 width=720 style='border-collapse:
 collapse;table-layout:fixed;width:540pt'>
 <col width=80 span=9 style='width:60pt'>
 <tr height=40 style='height:30.0pt'>
  <td height=40 width=80 style='height:30.0pt;width:60pt'></td>
  <td width=80 style='width:60pt'><strong>Collection</strong></td>
  <td width=80 style='width:60pt'><strong>List</strong></td>
  <td width=80 style='width:60pt'><strong>Set</strong></td>
  <td width=80 style='width:60pt'><strong>Map</strong></td>
  <td class=xl66 width=80 style='width:60pt'><strong>Collection.<br>iterator</strong></td>
  <td class=xl66 width=80 style='width:60pt'><strong>List.<br>iterator</strong></td>
  <td class=xl66 width=80 style='width:60pt'><strong>List.<br>listIterator</strong></td>
  <td class=xl66 width=80 style='width:60pt'><strong>Set.<br>iterator</strong></td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>add()</strong></td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>addAll()</strong></td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>clear()</strong></td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>remove()</strong></td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>removeAll()</strong></td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>retainAll()</strong></td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>add(pos,value)</strong></td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>addAll(pos,col)</strong></td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>set(pos,val)</strong></td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>put(key,val)</strong></td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>putAll(map)</strong></td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 class=xl65 style='height:15.0pt'><strong>set(value)</strong></td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
 </tr>
</table>

 
 
 
 
 
 
 
 