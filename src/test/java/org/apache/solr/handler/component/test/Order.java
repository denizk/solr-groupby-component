package org.apache.solr.handler.component.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class Order {

    public String date;

    public List<Transaction> transactions = new ArrayList<Transaction>();

    public Address address = new Address();

    public SolrInputDocument asDocument() {
        SolrInputDocument d = new SolrInputDocument();
        SolrInputDocument last = null;
        for (Transaction p : this.transactions) {
            last = p.asDocument();
            d.addChildDocument(last);
        }
        d.setField("noun", "order");
        d.setField("order_date", date);
        d.setField("order_city_name", address.city);
        d.setField("order_state_name", address.state);
        return d;
    }

}
