/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.training.person.query.service;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.impala.extdatasource.thrift.TBinaryPredicate;
import com.cloudera.impala.extdatasource.thrift.TCloseParams;
import com.cloudera.impala.extdatasource.thrift.TColumnDesc;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetStatsParams;
import com.cloudera.impala.extdatasource.thrift.TGetStatsResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TUniqueId;


public class ParametersBuilder {
    public static String   	TABLENAME = "impalaTest_users";
    public static int 	 	BATCHSIZE = 5;
    
	/*******************************************************************
	// Parameters to getStats().
	struct TGetStatsParams {
	  // The name of the table.
	  1: required string table_name

	  // A string specified for the table that is passed to the external data source.
	  2: optional string init_string

	  // A list of conjunctive (AND) clauses, each of which contains a list of
	  // disjunctive (OR) binary predicates.
	  3: required list<list<TBinaryPredicate>> predicates
	}

	// Returned by getStats().
	struct TGetStatsResult {
	  1: required ImpalaTypes.TStatus status

	  // Estimate of the total number of rows returned when applying the predicates indicated
	  // by accepted_conjuncts. Not set if the data source does not support providing
	  // this statistic.
	  2: optional i64 num_rows_estimate

	  // Accepted conjuncts. References the 'predicates' parameter in the getStats()
	  // call. It contains the 0-based indices of the top-level list elements (the
	  // AND elements) that the library will be able to apply during the scan. Those
	  // elements that arenâ€™t referenced in accepted_conjuncts will be evaluated by
	  // Impala itself.
	  3: list<i32> accepted_conjuncts
	}
	**********************************************************************/
	public static TPrepareParams ConstructGetStatsParams(
		List<List<TBinaryPredicate>> test_predicate
	) 
	{
            
            TPrepareParams params = new TPrepareParams();
            // (1) set table_name
            params.setTable_name(TABLENAME);
            // (2) set init_string
            params.setInit_string("init_string");
            // (3) set predicates	
            params.setPredicates(test_predicate);

            return params;
	}
        

	/*********************************************
	 * Construct accepted predicate based on the getStatsResult and original 
	 * test_predicate
	 * 
	 * @param test_predicate
	 * @param getStatsResult
	 * @return
	 */
	public static List<List<TBinaryPredicate>> ConstructAcceptedPredicate(
		List<List<TBinaryPredicate>> 	test_predicate,
		List<Integer>					accepted_conjuncts
		//TGetStatsResult 				getStatsResult
	) 
	{
		//List<Integer>	accepted_conjuncts = getStatsResult.getAccepted_conjuncts();
				
		List<List<TBinaryPredicate>>	supported_predicates = new ArrayList<List<TBinaryPredicate>>();
		for (Integer index : accepted_conjuncts) {
			supported_predicates.add(test_predicate.get(index));
		}
		return supported_predicates;
	}
	
	
	public static TTableSchema	ConstructTableScheme() {
		List<TColumnDesc>	colDescList = new ArrayList<TColumnDesc>();
		
		colDescList.add(new TColumnDesc("user_id",    new TColumnType(TPrimitiveType.INT)));
		colDescList.add(new TColumnDesc("last_name",  new TColumnType(TPrimitiveType.STRING)));
		colDescList.add(new TColumnDesc("age",        new TColumnType(TPrimitiveType.INT)));
		TColumnType		genderType = new TColumnType(TPrimitiveType.STRING);
		//genderType.setLen(1);
		colDescList.add(new TColumnDesc("gender", genderType));
		colDescList.add(new TColumnDesc("employer",   new TColumnType(TPrimitiveType.STRING)));
		
		TTableSchema row_schema = new TTableSchema(colDescList);
		
		return row_schema;
	}
	
	/*******************************************************
	struct TOpenParams {
	  // A unique identifier for the query.
	  1: required ImpalaTypes.TUniqueId query_id

	  // The name of the table.
	  2: required string table_name

	  // A string specified for the table that is passed to the external data source.
	  3: required string init_string

	  // The authenticated user name.
	  4: required string authenticated_user_name

	  // The schema of the rows that the scan needs to return.
	  5: required TTableSchema row_schema

	  // The expected size of the row batches it returns in the subsequent getNext() calls.
	  6: required i32 batch_size

	  // A representation of the scan predicates that were accepted in the preceding
	  // getStats() call.
	  7: required list<list<TBinaryPredicate>> predicates
	}
	************************************************************/
        public static TOpenParams ConstructOpenParams(
                List<List<TBinaryPredicate>> predicate)
        {
            TOpenParams params = new TOpenParams();

            // (1) set query_id
            TUniqueId queryId = new TUniqueId();
            queryId.setHi(System.currentTimeMillis());
            queryId.setLo(System.nanoTime());
            params.setQuery_id(queryId);

            // (2) set table_name
            params.setTable_name(TABLENAME);
                    //params.setTable_nameIsSet(true);

            // (3) set init_string
            params.setInit_string("init_string");

            // (4) set authenticated_user_name
            params.setAuthenticated_user_name("Roger");

            // (5) set row_schema
            params.setRow_schema(ConstructTableScheme());

            // (6) set batch_size
            params.setBatch_size(BATCHSIZE);

            // (7) set list<list<TBinaryPredicate>> predicates
            params.setPredicates(predicate);

            return params;
        }

	public static TGetNextParams ConstructGetNextParams(TOpenResult openResult) {
		TGetNextParams	params = new TGetNextParams(openResult.getScan_handle());
		return params;
	}
	
	public static TCloseParams ConstructCloseParams(TOpenResult openResult) {
		TCloseParams	params = new TCloseParams(openResult.getScan_handle());
		return params;
	}
}
