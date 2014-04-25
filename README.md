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

Given each city, get a breakdown of all users in that city, returning only the distinct meta-data (total, distinct).

##### Data Set

| id | state   | city    | user_id | spend |
|----|---------|---------|---------|-------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  |

##### Query

	?q=*:*&groupby=city,user_id
          &groupby.distinct=true

##### Results

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

### Aggregate Total Purchase Amount by City

Example shows aggregating at a single level, in this case city, to get the total spend in various cities.

##### Data Set

| id | state   | city    | user_id | spend |
|----|---------|---------|---------|-------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  |

##### Query

	?q=*:*&groupby=type
          &groupby.stats=amount

##### Results

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


### Aggregate Total Purchase Amount by State and City

Example shows multi-dimensional aggregates, this time including state and city in the group by to get the rollups (note: the data itself is flat and can be aggregated in many ways)

##### Data Set

| id | state   | city    | user_id | spend |
|----|---------|---------|---------|-------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  |

##### Query 

    ?q=*:*&groupby=city
          &groupby.stats=amount

##### Results

	{
	  "group": [
	    {
	      "state": [
	        {
	          "value": "FLORIDA",
	          "count": 3,
	          "stats": {
	            "spend": {
	              "sum": 11,
	              "count": 3
	            }
	          },
	          "group": {
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
	        }
	      ]
	    }
	  ]
	}


##### Block Join Syntax
    ?q=*:*&groupby=noun:order/state,noun:order/city \
          &groupby.stats=noun:xact/amount


### Pivot Spend by State,City,Category

Just like the above example, but this time showing that the nesting is arbitrary including category in the mix to get total spend.

##### Data Set

| id | state   | city    | user_id | spend | category |
|----|---------|---------|---------|-------|----------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  | CAR      |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   | CAR      |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  | FOOD     |
| 4  | FLORIDA | ORLANDO | 88888   | 1.50  | FOOD     |

##### Query

	?q=*:*&groupby=state,city,category
		  &groupby.stats=spend


##### Results

	{
	  "group": [
	    {
	      "state": [
	        {
	          "value": "FLORIDA",
	          "count": 4,
	          "stats": {
	            "spend": {
	              "sum": 12.5,
	              "count": 4
	            }
	          },
	          "group": {
	            "city": [
	              {
	                "value": "ORLANDO",
	                "count": 3,
	                "stats": {
	                  "spend": {
	                    "sum": 11,
	                    "count": 3
	                  }
	                },
	                "group": {
	                  "category": [
	                    {
	                      "value": "FOOD",
	                      "count": 2,
	                      "stats": {
	                        "spend": {
	                          "sum": 3,
	                          "count": 2
	                        }
	                      }
	                    },
	                    {
	                      "value": "CAR",
	                      "count": 1,
	                      "stats": {
	                        "spend": {
	                          "sum": 8,
	                          "count": 1
	                        }
	                      }
	                    }
	                  ]
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
	                },
	                "group": {
	                  "category": [
	                    {
	                      "value": "CAR",
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
	              }
	            ]
	          }
	        }
	      ]
	    }
	  ]
	}

##### Block Join Syntax Example

    ?q=*:*&groupby=noun:order/state,noun:order/city,noun:xact/category \
          &groupby.stats=noun:xact/amount

### Spend Percentile Breakdowns

Allow a user to specify the percentile breakdown to get the 25%,50%,75%, or custom percentile breakdown. Breakdowns are provided by [StreamLib - QDigest](https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/quantile/QDigest.java).

> Quantiles are points taken at regular intervals from the cumulative distribution function (CDF) of a random variable. Useful for answering questions like what is the total spend # that represents the 90th percentile of all spend within a populous. Practical applications including ranking/distribution/bucketing (heavy, medium, light, etc).

##### Data Set

| id | state   | city    | user_id | spend | category |
|----|---------|---------|---------|-------|----------|
| 1  | FLORIDA | TAMPA   | 99999   | 1.50  | CAR      |
| 2  | FLORIDA | ORLANDO | 11111   | 8.0   | CAR      |
| 3  | FLORIDA | ORLANDO | 99999   | 1.50  | FOOD     |
| 4  | FLORIDA | ORLANDO | 88888   | 1.50  | FOOD     |

##### Query

	?q=*:*&groupby=state
		  &groupby.stats=spend
		  &groupby.percentiles=25,50,75

##### Result

	{
	  "group": [
	    {
	      "state": [
	        {
	          "value": "FLORIDA",
	          "count": 4,
	          "stats": {
	            "spend": {
	              "sum": 12.5,
	              "count": 4,
	              "percentile-25": 4,
	              "percentile-50": 8,
	              "percentile-75": 8
	            }
	          }
	        }
	      ]
	    }
	  ]
	}

> Reading this result, the 25%-tile for spend in the state of florida was ~$4.00, that 50% of the time the spend was $8.00, and that 75% of the time the spend was $8.00. Obviously with larger sets of data this data will stratify more.

##### Block Join Syntax

    ?q=*:*&groupby=noun:xact/category \
          &groupby.stats=noun:xact/amount
          &groupby.percentiles=25,50,75


### Returning multiple aggregates

Aggregates can be return at any level and in multiples so that we can get the overall aggregates for spend, quantity, and other measures in one single shot.

##### Example

	?q=*:*&groupby=category
		  &groupby.stats=spend
		  &groupby.stats=quantity
          &groupby.percentiles=25,50,75

This will yield the category/spend and category/quantity totals with their quantile breakdown in a single call.

##### Block Join Syntax

    ?q=*:*&groupby=noun:xact/category \
          &groupby.stats=noun:xact/amount
          &groupby.stats=noun:xact/quantity
          &groupby.percentiles=25,50,75

In the above contrived example we return the aggregate $ spend and # of purchases in each category along with the percentiles for each.

### Date Range Group By

Doing comparative analysis of prior week vs. this week is a common analytic function when doing click stream analytics or year/year metric evaluation. We support date range group by using the specified syntax.

##### Data Set

| id | user_id | dt                   | event      |
|----|---------|----------------------|------------|
| 1  | 1111    | 2014-01-01T12:32:12Z | click      |
| 1  | 1111    | 2014-01-01T12:32:13Z | impression |
| 1  | 1111    | 2014-01-10T15:23:13Z | conversion |

##### Query

	?q=*:*&groupby=dt,user_id
		  &groupby.distinct=true
		  &groupby.range.dt.start=NOW/DAY-14DAYS
		  &groupby.range.dt.end=NOW/DAY
		  &groupby.range.gap=+7DAYS

##### Result

	{
	  "group": [
	    {
	      "event": [
	        {
	          "value": "click",
	          "count": 1,
	          "group": {
	            "dt": [
	              {
	                "value": "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]",
	                "range.start": "2014-01-01T00:00:00Z",
	                "range.stop": "2014-01-08T00:00:00Z",
	                "count": 1,
	                "group": {
	                  "user_id": {
	                    "value": "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]",
	                    "path": "event:click/dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]",
	                    "unique": 1,
	                    "total": 1,
	                    "join": {
	                      "conversion": {
	                        "dt:[2014-01-08T00:00:00Z TO 2014-01-15T00:00:00Z]": {
	                          "intersect": 1,
	                          "union": 1,
	                          "total": 2
	                        }
	                      },
	                      "impression": {
	                        "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]": {
	                          "intersect": 1,
	                          "union": 1,
	                          "total": 2
	                        }
	                      }
	                    }
	                  }
	                }
	              }
	            ]
	          },
	          "join": {
	            "conversion": {
	              "intersect": 1,
	              "union": 1,
	              "total": 2
	            },
	            "impression": {
	              "intersect": 1,
	              "union": 1,
	              "total": 2
	            }
	          }
	        },
	        {
	          "value": "conversion",
	          "count": 1,
	          "group": {
	            "dt": [
	              {
	                "value": "dt:[2014-01-08T00:00:00Z TO 2014-01-15T00:00:00Z]",
	                "range.start": "2014-01-08T00:00:00Z",
	                "range.stop": "2014-01-15T00:00:00Z",
	                "count": 1,
	                "group": {
	                  "user_id": {
	                    "value": "dt:[2014-01-08T00:00:00Z TO 2014-01-15T00:00:00Z]",
	                    "path": "event:conversion/dt:[2014-01-08T00:00:00Z TO 2014-01-15T00:00:00Z]",
	                    "unique": 1,
	                    "total": 1,
	                    "join": {
	                      "click": {
	                        "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]": {
	                          "intersect": 1,
	                          "union": 1,
	                          "total": 2
	                        }
	                      },
	                      "impression": {
	                        "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]": {
	                          "intersect": 1,
	                          "union": 1,
	                          "total": 2
	                        }
	                      }
	                    }
	                  }
	                }
	              }
	            ]
	          },
	          "join": {
	            "click": {
	              "intersect": 1,
	              "union": 1,
	              "total": 2
	            },
	            "impression": {
	              "intersect": 1,
	              "union": 1,
	              "total": 2
	            }
	          }
	        },
	        {
	          "value": "impression",
	          "count": 1,
	          "group": {
	            "dt": [
	              {
	                "value": "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]",
	                "range.start": "2014-01-01T00:00:00Z",
	                "range.stop": "2014-01-08T00:00:00Z",
	                "count": 1,
	                "group": {
	                  "user_id": {
	                    "value": "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]",
	                    "path": "event:impression/dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]",
	                    "unique": 1,
	                    "total": 1,
	                    "join": {
	                      "click": {
	                        "dt:[2014-01-01T00:00:00Z TO 2014-01-08T00:00:00Z]": {
	                          "intersect": 1,
	                          "union": 1,
	                          "total": 2
	                        }
	                      },
	                      "conversion": {
	                        "dt:[2014-01-08T00:00:00Z TO 2014-01-15T00:00:00Z]": {
	                          "intersect": 1,
	                          "union": 1,
	                          "total": 2
	                        }
	                      }
	                    }
	                  }
	                }
	              }
	            ]
	          },
	          "join": {
	            "click": {
	              "intersect": 1,
	              "union": 1,
	              "total": 2
	            },
	            "conversion": {
	              "intersect": 1,
	              "union": 1,
	              "total": 2
	            }
	          }
	        }
	      ]
	    }
	  ]
	}

## Full list of parameters

| param                                 | value                 | description                                                                                                                                                                                                                                                                                 |
|---------------------------------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| groupby                               | [field,field,field]   | The field(s) to group by                                                                                                                                                                                                                                                                    |
| groupby.stats                         | field               | The field to perform statistics against, this field should be int, double, float, etc                                                                                                                                                                                                       |
| groupby.distinct                      | [true|false]          | If you would like the last value in your groupby to rolled up into a simple count of distinct / total.                                                                                                                                                                                      |
                                                                                                                                                                                                                                           
| groupby.limit                         | [0-9+]                | If you want to provide a limit on the groups to return, any group with less than this number will be filtered out.                                                                                                                                                                          |
| groupby.range.<field>.start           | [DateFormat]          | The start date for the date range, uses solr's built in date syntax parser. For example NOW/DAY, NOW/YEAR, NOW/MONTH (to round today to nearest day, week, month, year, hour, etc). And also the date math like +1DAY, +3 DAYS, +1YEAR, +1HOUR, etc.                                        |
| groupby.range.<field>.end             | [DateFormat]          | The end date for the date range. For example NOW/DAY, NOW/YEAR, NOW/MONTH (to round today to nearest day, week, month, year, hour, etc). And also the date math like +1DAY, +3 DAYS, +1YEAR, +1HOUR, etc.                                                                                   |
| groupby.range.<field>.gap             | [DateMath]            | The date math to specify the gap between dates, for example +1DAY, +7DAYS, +1HOUR, etc.                                                                                                                                                                                                     |
| cardinality.estimate.size             | [0-9+]                | The integer that represents the tipping point where the system will starting doing approximations of cardinality. Trading speed/memory for accuracy. Uses HyperLogLog by default and by default will tip at 1,000 unique items.                                                             |
| cardinality.estimate.eps              | [0.00-1.00]           | The error percentage allowed, the higher the number the faster and lower memory used, the lower the number the more memory used but more accurate the estimates are. Uses HyperLogLog by default and will default to an EPS of 0.05. See streamlib HyperLogLog for more information.        |
| groupby.minimize                      | [true,false]          | Will filter out any groups with a count <= 0. Useful for trimming date ranges that have no data, and by default is set to false. If you want to optimize bandwidth, set to false. Set to true by default as most charting libraries require every date to be provided and can't infer gaps. |
| groupby.stats.percentiles             | <0,10,20,...>         | The list of percentiles you want to generate when running stats. This will use QDigest to generate aproximate quantile breakdowns by the specified stat field.                                                                                                                              |
| groupby.stats.percentiles.compression | [0-9+]                | The compression to use (sets the compression on the QDigest) which trades accuracy for speed. See QDigest implementation in StreamLib.                                                                                                                                                      |
| groupby.having                        | sum(spend):[10 * 100] | Unstable - allows only returning groups which have a sum(<field>) between the specified ranges.                                                                                                                                                                                             |

## Block Join Specific Niceness

Do to the nature of block join syntax being rather obtuse the following allows you to use filter queries that read left-to-right for block-joins without the nasty query syntax block joins require.

#### Aggregate with Constraint

    ?q=*:*&groupby=noun:order/city:CLINTON \
          &groupby.stats=noun:xact/amount

We can apply constraints to the group by to allow us to only return back a filtered view of groups. In this example, we get the total spend for a particular city name - [keep in mind "CLINTON" is the #1 most duplicated city name](http://voices.yahoo.com/duplicate-name-cities-usa-kind-towns-3638487.html) - so if we had a full index we would be actually asking for multiple cities.

Don't worry, we can get more specific!

    ?q=*:*&groupby=noun:order/state:UTAH,noun:order/city:CLINTON \
          &groupby.stats=noun:xact/amount

Now we will only get back the aggregates for CLINTON in the state of UTAH. Or if we wanted we could just pull back all states with a city of "CLINTON" by removing our constraint we just added and keeping state.

    ?q=*:*&groupby=noun:order/state,noun:order/city:CLINTON \
          &groupby.stats=noun:xact/amount

### Sanity Check - What query syntax is that? ##

At this point you are probably wondering what the heck this syntax is. It kinda-looks like XPath, but it has what looks to be XML Namespaces attached to them. Let's break it down.

#### Why it was needed

If you index document using flattened structures, you can ignore the syntax entirely. In-fact, you should probably just use facets and stats components if you are in this camp and stop here.

However, if you have documents which by-their-nature are hierarchical in form and function you may like to know that SOLR 4.5+ includes a [Block Join Support](https://issues.apache.org/jira/browse/SOLR-3076). If you haven't heard of it, well, it's amazing - read up!

Block Join [has very performance numbers](http://blog.griddynamics.com/2012/08/block-join-query-performs.html) and indexing is really easy. Simply create a SolrInputDocument and than add your children to that document, and add children to those children, etc.

That being said, querying is blindingly obtuse. For good reason, SOLR never understood hierarchies, and so the syntax hasn't caught up. For right now you can either use this module or issue rather complex and crazy queries like this one...

    q=+{!parent which=type_s:product v=$skuq} +{!parent which=type_s:product v=$vendorq} \ 
    &skuq=+COLOR_s:Blue +SIZE_s:XL +{!parent which=type_s:sku v='+QTY_i:[10 TO *] +STATE_s:CA'} \ 
    &vendorq=+NAME_s:Bob +PRICE_i:[20 TO 25]

I don't think ANYONE wants to write queries like that (or even generate them) - [this guy did and gives good background](http://blog.griddynamics.com/2013/12/grandchildren-and-siblings-with-block.html). And there is even [a JIRA ticket](https://issues.apache.org/jira/browse/LUCENE-5375) for this too.

What we need is something familiar and simple - a little abstraction magic and we should be good.

#### How it works

We'll because the root problem is we are constructing joins and because we **WANT** to query hierarchies we need a slight twist on the language. By all means, this is a `SearchComponent`, it doesn't mess with the existing search parameters - however it also doesn't try to pretend it's the same as normal querying. This is good and bad..

**The good:**

+ You can issue both `groupby` commands and normal solr queries like `facet`, `stats`, `q`, and others just fine and even in the same REST call.
+ You don't have to recompile SOLR to get this new feature, just grab the JAR and register it in any SOLR version from 4.5 and above.
+ Its just as performant as a native component.
+ Different things should be different.

**The bad:**

+ It doesn't share any state between queries which means if you facet, your going to have to restate your facet query as a groupby query to get the same result.
+ It is different - and different can be a stumbling block for some.

#### Syntax Breakdown

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