package org.apache.solr.handler.component.test;

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

public class Transaction {

    public Float amount;

    public Integer quantity;

    public String upc;

    public String category;

    public String subcategory;

    public String manufacturer;

    public String corporation;

    public String brand;

    public String type;

    public String subtype;

    public Boolean private_label;

    public SolrInputDocument asDocument() {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("noun", "xact");
        d.addField("product_brand_name", this.brand);
        d.addField("product_category_name", this.category);
        d.addField("product_purchase_quantity", this.quantity);
        d.addField("product_purchase_amount", this.amount);
        return d;
    }
}