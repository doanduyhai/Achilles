## Collections and Maps support

 **Achilles** support mapping for Set, List and Map types. The underlying implementation returned by a *managed*
 entity is a HashSet, ArrayList and HashMap.
 
 Example:
 
	@Entity
	@ColumnFamily
	public class PlayerStats implements Serializable
	{
		private static final long serialVersionUID = 1L;

		@Id
		private Long id;

		@Column
		private Set<Rank> earnedRanks;
		
		@Lazy
		@Column
		private List<String> favoriteItems;

		@Column
		private Map<Long,Integer> levelsScore;		
	} 


 All collections and maps fields are eagerly fetched by default when the entity is loaded. However you can make
 them lazy with the  *@Lazy* annotation. A *lazy* collection or map is not loaded when the entity is fetched but
 only upon getter invocation.
 
>	Even if set as *lazy*, collection or map values are loaded entirely in memory. Therefore we strongly advise 
	to limit the use of collections and maps for **small number of items (<1000)**.
 
 
 If you need to store more than 1000 items, consider using **internal** or **external wide maps**. Check [Internal WideMap][internalWideMap]
 and [External WideMap][externalWideMap] for more details

[internalWideMap]: /doanduyhai/achilles/tree/master/documentation/internalWideMap.markdown
[externalWideMap]: /doanduyhai/achilles/tree/master/documentation/externalWideMap.markdown
 
 
 
 