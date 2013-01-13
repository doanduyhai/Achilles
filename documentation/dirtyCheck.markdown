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
 
 <table border=0 cellpadding=0 cellspacing=0 width=1449 style='border-collapse:
 collapse;table-layout:fixed;width:1088pt'>
 <col width=174 span=2 style='mso-width-source:userset;mso-width-alt:6363;
 width:131pt'>
 <col width=80 span=6 style='width:60pt'>
 <col width=105 style='mso-width-source:userset;mso-width-alt:3840;width:79pt'>
 <col width=149 style='mso-width-source:userset;mso-width-alt:5449;width:112pt'>
 <col width=99 style='mso-width-source:userset;mso-width-alt:3620;width:74pt'>
 <col width=101 style='mso-width-source:userset;mso-width-alt:3693;width:76pt'>
 <col width=87 style='mso-width-source:userset;mso-width-alt:3181;width:65pt'>
 <col width=80 style='width:60pt'>
 <tr height=20 style='height:15.0pt'>
  <td height=20 width=174 style='height:15.0pt;width:131pt'></td>
  <td width=174 style='width:131pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=105 style='width:79pt'></td>
  <td width=149 style='width:112pt'></td>
  <td width=99 style='width:74pt'></td>
  <td width=101 style='width:76pt'></td>
  <td width=87 style='width:65pt'></td>
  <td width=80 style='width:60pt'></td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 colspan=14 style='height:15.0pt;mso-ignore:colspan'></td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>Type/Supported operation</td>
  <td class=xl67>add()</td>
  <td class=xl67>addAll()</td>
  <td class=xl67>clear()</td>
  <td class=xl67>remove()</td>
  <td class=xl67>removeAll()</td>
  <td class=xl67>retainAll()</td>
  <td class=xl67>add(pos,value)</td>
  <td class=xl67>addAll(pos,collection)</td>
  <td class=xl67>set(pos,value)</td>
  <td class=xl67>put(key,value)</td>
  <td class=xl67>putAll(map)</td>
  <td class=xl67>set(value)</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>Collection</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>List</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>Set</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>Map</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>Collection.iterator</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>List.iterator</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>List.listIterator</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
 </tr>
 <tr height=20 style='height:15.0pt'>
  <td height=20 style='height:15.0pt'></td>
  <td class=xl66>Set.iterator</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>YES</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
  <td class=xl65>N/A</td>
 </tr>
 <tr height=0 style='display:none'>
  <td width=174 style='width:131pt'></td>
  <td width=174 style='width:131pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=80 style='width:60pt'></td>
  <td width=105 style='width:79pt'></td>
  <td width=149 style='width:112pt'></td>
  <td width=99 style='width:74pt'></td>
  <td width=101 style='width:76pt'></td>
  <td width=87 style='width:65pt'></td>
  <td width=80 style='width:60pt'></td>
 </tr>
</table>

 
 
 
 
 
 
 
 