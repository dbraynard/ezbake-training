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

import com.cloudera.impala.extdatasource.thrift.TBinaryPredicate;
import com.cloudera.impala.extdatasource.thrift.TCloseParams;
import com.cloudera.impala.extdatasource.thrift.TCloseResult;
import com.cloudera.impala.extdatasource.thrift.TColumnDesc;
import com.cloudera.impala.extdatasource.thrift.TComparisonOp;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.thrift.TRowBatch;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TStatusCode;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.query.basequeryableprocedure.client.Client;
import ezbake.training.PersonQueryService;
import static ezbake.training.person.query.service.ParametersBuilder.TABLENAME;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author developer
 */
public class PersonQueryServiceImpl extends EzBakeBaseThriftService
                                                implements PersonQueryService.Iface
{
    private static final Logger logger = LoggerFactory.getLogger(PersonQueryServiceImpl.class);
    
    private ezbake.query.basequeryableprocedure.client.Client client;
    
    public PersonQueryServiceImpl()
    {        
        logger.info("The person query service was instantiated.");
    }
    
    @Override
    public TProcessor getThriftProcessor() 
    {
        return new PersonQueryService.Processor<>(this);        
    }
   
    public boolean ping() {
        return true;
    }
    
     
    public List<List<TBinaryPredicate>> ConstructPredicates_01()
    {
        List<List<TBinaryPredicate>> predicates = new ArrayList<List<TBinaryPredicate>>();
        List<TBinaryPredicate> binaryPredicateList1 = new ArrayList<TBinaryPredicate>();

        //TColumnDesc colDesc_1 = new TColumnDesc("employer", new TColumnType(TPrimitiveType.STRING));
        TColumnDesc colDesc_1 = new TColumnDesc();
        colDesc_1.setName("employer");
        colDesc_1.setType(new TColumnType(TPrimitiveType.STRING));

        TColumnValue colValue_1 = new TColumnValue();
        colValue_1.setString_val("Cloudera");
        //colValue_1.setStringVal("Cloudera");
        //TBinaryPredicate p_1 = new TBinaryPredicate(colDesc_1, TComparisonOp.EQ, colValue_1);
        TBinaryPredicate p_1 = new TBinaryPredicate();
        p_1.setCol(colDesc_1);
        p_1.setOp(TComparisonOp.EQ);
        p_1.setValue(colValue_1);

        binaryPredicateList1.add(p_1);
        predicates.add(binaryPredicateList1);

        return predicates;
    }
    
    @Override
    public Set<String> getPersons(String lastname, EzSecurityToken securityToken) throws TException
    {
        TOpenParams openParams = new TOpenParams();
        
        client = new Client();
        
        //client.open(null);
        
        
        List<List<TBinaryPredicate>> accepted_predicate;
        TPrepareParams prepareParams;
        TPrepareResult prepareResult;

        TOpenParams openParams;
        TOpenResult openResult;

        TGetNextParams getNextParams;
        TGetNextResult getNextResult;
        boolean eos = false;
        TStatusCode statusCode;

        TCloseParams closeParams;
        TCloseResult closeResult;
 
        List<List<TBinaryPredicate>> test_predicate = ConstructPredicates_01();

        // step 1 -- prepare()
        prepareParams = ParametersBuilder.ConstructGetStatsParams(test_predicate);
        prepareResult = client.prepare(prepareParams);        
        
        
        System.out.println("client.getStats() returned : \n" + prepareResult);
        
        // construct accepted predicates from getStatsResult
        accepted_predicate = ParametersBuilder.ConstructAcceptedPredicate(
                test_predicate,
                prepareResult.getAccepted_conjuncts()
        );

        // step 2 -- open()
        openParams = ParametersBuilder.ConstructOpenParams(accepted_predicate);
        openResult = client.open(openParams);
        System.out.println("client.open() returned :\n" + openResult);

        // step 3 -- getNext() until the end of table
        getNextParams = ParametersBuilder.ConstructGetNextParams(openResult);
        do
        {
            getNextResult = client.getNext(getNextParams);
            System.out.println("client.getNext() returned :\n" + getNextResult);
            statusCode = getNextResult.getStatus().getStatus_code();
            if (TStatusCode.OK != statusCode)
            {
                break;
            }
            eos = getNextResult.isEos();
            TRowBatch rowBatch = getNextResult.getRows();
            
            List<TRow> rowList = rowBatch.getRows();

            System.out.println("client.getNext() returned Rows:");
            for (TRow row : rowList)
            {
                List<TColumnValue> valueList = row.getCol_vals();
                //for (TColumnValue value)
                System.out.println(valueList);
            }
        } while (false == eos);
        
       
        // step 4 -- close()
        closeParams = ParametersBuilder.ConstructCloseParams(openResult);
        closeResult = client.close(closeParams);
        System.out.println("client.close() returned :\n" + closeResult);
        
    }   
   



    
    
}
