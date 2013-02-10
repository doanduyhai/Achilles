## Manual Column Family Creation

 If you want to create manually all the column families instead of letting **Achilles** do it for you, you can active the 
 log level of `info.archinnov.achilles.columnFamily.ColumnFamilyBuilder` to **DEBUG**. The creation script will be displayed:
 
	Create Dynamic Composite-based column family for entity 'integration.tests.entity.CompleteBean' : 
		create column family CompleteBean
			with key_validation_class = LongType
			and comparator = 'DynamicCompositeType(a=>AsciiType,b=>BytesType,c=>BooleanType,d=>DateType,e=>DecimalType,z=>DoubleType,f=>FloatType,i=>IntegerType,j=>Int32Type,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType)'
			and default_validation_class = UTF8Type
			and comment = 'Column family for entity integration.tests.entity.CompleteBean'


	Create Composite-based column family for property 'multiKeyExternalWideMap' of entity 'integration.tests.entity.CompleteBean' : 
		create column family MultiKeyExternalWideMap
			with key_validation_class = LongType
			and comparator = 'CompositeType(UTF8Type,UUIDType)'
			and default_validation_class = UTF8Type		
			and comment = 'Column family for property multiKeyExternalWideMap of entity integration.tests.entity.CompleteBean'


	Create Composite-based column family for property 'map' of entity 'integration.tests.entity.MultiKeyColumnFamilyBean' : 
		create column family MultiKeyColumnFamilyBean
			with key_validation_class = LongType
			and comparator = 'CompositeType(LongType,UTF8Type)'
			and default_validation_class = UTF8Type		
			and comment = 'Column family for property map of entity integration.tests.entity.MultiKeyColumnFamilyBean'
			

 As shown above, all column families supporting an entity has a DynamicComposite comparator type with a list of aliases to indicates
 the dynamic type at runtime.
 
 

  