solr-groupby-component
======================

A search component that allows group by, aggregate, and estimate queries.

# Installation #

This component implements a custom SearchComponent and can be registered in your solr.config.

    
    <searchComponent name="groupby" class="org.apache.solr.handler.component.aggregates.GroupByComponent"/>
    <requestHandler name="standard" class="solr.StandardRequestHandler">
        <bool name="httpCaching">true</bool>
        <arr name="last-components">
            <str>groupby</str>
        </arr>
    </requestHandler>

# Disclaimer #

> This component is released under apache licenese. Use at your own discretion, this author takes no liability for anything, at all, ever and makes no guarantees about the software provided.

# Latest #

+ Updated API to support date range group bys (useful for time-window based group bys, for example sliding window analysis, etc)
+ Updated API to support regular lucene fields
+ Updated API to support CountThenSketch(...) with provided EPS rates
+ Updated API to support intersection/union cross-joins - such that when you group by attributes you get a "pivot-like" breakdown of all permutations. Useful for things like click -> conversion analysis.

# Next Releases #

+ Distributed Support (QDigest, HyperLogLog, etc)
+ Add Distinct(...), Having(...)
+ Aggregate Filters (already checked in but need some tweaks)

# What it can do #

That being said it can provide some fairly nice pivot trees with percentiles, sum, and averages. It also has features to allow for facet/pivot on Block Join documents.

## Examples ##

The example provided assumes indexing of Solr Block Join documents representing consumer profiles. See the example docs for illustration. The test data represents a Hierarchical object:

+ Shopper
+ Order(s)
+ Transactions(s)

Simple model, rather standard in e-commerce.  When indexing the JSON documents the new `SolrInputDocument.addChild(...)` call is used. Also a marker is put on each document in the form of a key=pair, "noun=object". Each document will then have a 'noun' field mapping to what type of document it is in the nested structure. Shopper = `noun:shopper`, Order = `noun:order`, Transaction = `noun:xact`. This is important for block-join hints as we will show.

## Query ##

## Groups ##

Basically the same thing as pivot, but with a twist, in that we support `distinct` and `union`/`intersection` logic. If just doing "groupby" with no additional arguments than we get exactly the same result as pivot (save the name is now 'group'), and functions the same. In the future the difference will be that group will support distributed queries with an option to specify accuracy (hyperloglog/countminsketch/etc).

### Distinct users by city

#### Data Set

| id | state   | city    | user_id | spend |
|----|---------|---------|---------|-------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  |

Given each city, get a breakdown of all users in that city, returning only the distinct meta-data (total, distinct).

	?q=*:*&groupby=city,user_id
          &groupby.distinct=true

Yields:

	{
	  "group":[
	    {
	      "city":[
	        {
	          "value":"ORLANDO",
	          "count":2,
	          "group":{
	            "user_id":{
	              "value":"ORLANDO",
	              "path":"city:ORLANDO",
	              "unique":2,
	              "total":2,
	              "join":{
	                "TAMPA":{
	                  "intersect":1,
	                  "union":2,
	                  "total":3
	                }
	              }
	            }
	          }
	        },
	        {
	          "value":"TAMPA",
	          "count":1,
	          "group":{
	            "user_id":{
	              "value":"TAMPA",
	              "path":"city:TAMPA",
	              "unique":1,
	              "total":1,
	              "join":{
	                "ORLANDO":{
	                  "intersect":1,
	                  "union":2,
	                  "total":3
	                }
	              }
	            }
	          }
	        }
	      ]
	    }
	  ]
	}

Note: You get the intersection of TAMPA & ORLANDO as well as union and total. So we can see that TAMPA shares one user with ORLANDO.

### Aggregate Total Purchase Amount
 
#### Data Set

| id | state   | city    | user_id | spend |
|----|---------|---------|---------|-------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  |

Regular syntax - just give the field and the stats you want.

	?q=*:*&groupby=type
          &groupby.stats=amount

Results in...

	{
	  "group": [
	    {
	      "city": [
	        {
	          "value": "ORLANDO",
	          "count": 2,
	          "stats": {
	            "spend": {
	              "sum": 9.5,
	              "count": 2
	            }
	          }
	        },
	        {
	          "value": "TAMPA",
	          "count": 1,
	          "stats": {
	            "spend": {
	              "sum": 1.5,
	              "count": 1
	            }
	          }
	        }
	      ]
	    }
	  ]
	}


Block join syntax version. This will yield a response which has the total spend of everyone that has been indexed.

    ?q=*:*&groupby=noun:shopper/noun
          &groupby.stats=noun:xact/amount


### Aggregate Total Purchase Amount by City

Regular syntax 

    ?q=*:*&groupby=city
          &groupby.stats=amount

This will return all unique cities with each city having the total amount of spend in that city.

    ?q=*:*&groupby=noun:order/city \
          &groupby.stats=noun:xact/amount


### Pivot/Aggregate Total Purchase Amount by State,City,Category

    ?q=*:*&groupby=noun:order/state,noun:order/city,noun:xact/category \
          &groupby.stats=noun:xact/amount

This will return all distinct states, with pivot into each city within that state, and each category of item purchased in that city with aggregate statistics for each level.

### Aggregate with Percentiles

    ?q=*:*&groupby=noun:xact/category \
          &groupby.stats=noun:xact/amount
          &groupby.percentiles=25,50,75

This will return the sum, count, and distribution of average spend for any given category.

### Returning multiple aggregates

Aggregates can be return at any level and in multiples.

    ?q=*:*&groupby=noun:xact/category \
          &groupby.stats=noun:xact/amount
          &groupby.stats=noun:xact/quantity
          &groupby.percentiles=25,50,75

In the above contrived example we return the aggregate $ spend and # of purchases in each category along with the percentiles for each.

### Aggregate with Constraint

    ?q=*:*&groupby=noun:order/city:CLINTON \
          &groupby.stats=noun:xact/amount

We can apply constraints to the group by to allow us to only return back a filtered view of groups. In this example, we get the total spend for a particular city name - [keep in mind "CLINTON" is the #1 most duplicated city name](http://voices.yahoo.com/duplicate-name-cities-usa-kind-towns-3638487.html) - so if we had a full index we would be actually asking for multiple cities.

Don't worry, we can get more specific!

    ?q=*:*&groupby=noun:order/state:UTAH,noun:order/city:CLINTON \
          &groupby.stats=noun:xact/amount

Now we will only get back the aggregates for CLINTON in the state of UTAH. Or if we wanted we could just pull back all states with a city of "CLINTON" by removing our constraint we just added and keeping state.

    ?q=*:*&groupby=noun:order/state,noun:order/city:CLINTON \
          &groupby.stats=noun:xact/amount

## Sanity Check - What query syntax is that? ##

At this point you are probably wondering what the heck this syntax is. It kinda-looks like XPath, but it has what looks to be XML Namespaces attached to them. Let's break it down.

### Why it was needed

If you index document using flattened structures, you can ignore the syntax entirely. In-fact, you should probably just use facets and stats components if you are in this camp and stop here.

However, if you have documents which by-their-nature are hierarchical in form and function you may like to know that SOLR 4.5+ includes a [Block Join Support](https://issues.apache.org/jira/browse/SOLR-3076). If you haven't heard of it, well, it's amazing - read up!

Block Join [has very performance numbers](http://blog.griddynamics.com/2012/08/block-join-query-performs.html) and indexing is really easy. Simply create a SolrInputDocument and than add your children to that document, and add children to those children, etc.

That being said, querying is blindingly obtuse. For good reason, SOLR never understood hierarchies, and so the syntax hasn't caught up. For right now you can either use this module or issue rather complex and crazy queries like this one...

    q=+{!parent which=type_s:product v=$skuq} +{!parent which=type_s:product v=$vendorq} \ 
    &skuq=+COLOR_s:Blue +SIZE_s:XL +{!parent which=type_s:sku v='+QTY_i:[10 TO *] +STATE_s:CA'} \ 
    &vendorq=+NAME_s:Bob +PRICE_i:[20 TO 25]

I don't think ANYONE wants to write queries like that (or even generate them) - [this guy did and gives good background](http://blog.griddynamics.com/2013/12/grandchildren-and-siblings-with-block.html). And there is even [a JIRA ticket](https://issues.apache.org/jira/browse/LUCENE-5375) for this too.

What we need is something familiar and simple - a little abstraction magic and we should be good.

### How it works

We'll because the root problem is we are constructing joins and because we **WANT** to query hierarchies we need a slight twist on the language. By all means, this is a `SearchComponent`, it doesn't mess with the existing search parameters - however it also doesn't try to pretend it's the same as normal querying. This is good and bad..

**The good:**

+ You can issue both `groupby` commands and normal solr queries like `facet`, `stats`, `q`, and others just fine and even in the same REST call.
+ You don't have to recompile SOLR to get this new feature, just grab the JAR and register it in any SOLR version from 4.5 and above.
+ Its just as performant as a native component.
+ Different things should be different.

**The bad:**

+ It doesn't share any state between queries which means if you facet, your going to have to restate your facet query as a groupby query to get the same result.
+ It is different - and different can be a stumbling block for some.

### Syntax Breakdown

Group by commands are always prefixed by a Block Join Hint.

    &groupby=${block_join_hint}/${field}[:${term,rangequery}]

**block_join_hint:**
The glue that makes it work. The block join hint is optional, it falls back to regular ol' SOLR queries if not provided. If you have a classic flattened SOLR index, feel free to skip this. This hint is used to build a `ToChildBlockJoinQuery` by taking this parameter as the implied parent, and the next field as the child query (it's more complex than that, but for now 80/20).

**field:**
The field you want to group by/facet over/aggregate. It is expected that this field be *AT THE SAME LEVEL OR BELOW* as the block join hint.

**term,rangequery,etc:**
If you seen in the examples, sometimes it is nice to constrain the group by, for example, group by states, but only Florida. This essentially allows us to issue pre-filters to the group join. This can also be done (but very painfully) using the standard `{!...}` block join syntax in SOLR - however - it is unfriendly and also doesn't work with aggregations (multi-level facets).

<pre>
?...&groupby=noun:order/state<b style="color: #f40000; text-decoration: underline;">:FLORIDA</b>,noun:xact/brand<b style="color: #f40000; text-decoration: underline;">:RED BULL</b>
   &groupby.stats=noun:xact/amount
</pre>

This will return the aggregate result of all purchases in the state of florida for RED BULL only.

#### Test Documents ####

    {
      "id" : 11111111,
      "orders" : [ 
    	  {
    	    "date" : 20130407,
    	    "address" : {
    	      "state" : "FLORIDA",
    	      "city" : "TAMPA",
    	      "county" : "HILLSBOROUGH",
    	      "postal_code" : "33634",
    	      "fips_code" : "12086",
    	      "market" : "TAMPA/ST. PETERSBURG",
    	      "region" : "SOUTHEAST",
    	      "latitude" : 25.808700561523438,
    	      "longitude" : -80.19509887695312
    	    },
    	    "transactions" : [
    	    	{
    		    	"quantity" : 1,
    		      	"amount" : 5.0,
    		      	"upc" : "12341234",
    		      	"category" : "ENERGY DRINKS",
    		      	"subcategory" : "SUPPLIMENTS",
    		      	"manufacturer" : "RED BULL NORTH AMERICA",
    		      	"corporation" : "RED BULL",
    		      	"brand" : "RED BULL",
    		      	"type" : "ENERGY DRINKS/SHOTS"
    	    	},
    	    	{
    		    	"quantity" : 1,
    		      	"amount" : 2.0,
    		      	"upc" : "444444444",
    		      	"category" : "ENERGY DRINKS",
    		      	"subcategory" : "SUPPLIMENTS",
    		      	"manufacturer" : "PEPSI",
    		      	"corporation" : "PEPSI",
    		      	"brand" : "MONSTER",
    		      	"type" : "ENERGY DRINKS/SHOTS"
    	    	},
    	    	{
    		    	"quantity" : 1,
    		      	"amount" : 2.0,
    		      	"upc" : "1222222222222",
    		      	"category" : "PROTEIN SHAKE",
    		      	"subcategory" : "SUPPLIMENTS",
    		      	"manufacturer" : "EAS",
    		      	"corporation" : "EAS",
    		      	"brand" : "EAS 100% WHEY PROTEIN",
    		      	"type" : "BODY BUILDER"
    	    	},
    	    	{
    		    	"quantity" : 1,
    		      	"amount" : 1.0,
    		      	"upc" : "55555555",
    		      	"category" : "CANDY",
    		      	"subcategory" : "CHEWY CANDY",
    		      	"manufacturer" : "MARS, INC",
    		      	"corporation" : "MARS, INC",
    		      	"brand" : "STARBURST",
    		      	"type" : "CANDY"
    	    	}
    	    ]
    	  }
      ]
    }