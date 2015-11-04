/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intropro.prairie.unit.oozie;

import com.intropro.prairie.unit.oozie.exception.OozieException;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

/**
 * Created by presidentio on 9/18/15.
 */
public class OozieJob {

    private static final int MAX_RETRIES = 5;
    private static final int CHECK_INTERVAL = 100;

    private String jobId;

    private org.apache.oozie.client.OozieClient oozieClient;

    public OozieJob(String jobId, org.apache.oozie.client.OozieClient oozieClient) {
        this.jobId = jobId;
        this.oozieClient = oozieClient;
    }

    public void waitFinish(long timeout) throws OozieException {
        long timeStart = System.currentTimeMillis();
        int reties = 0;
        while (System.currentTimeMillis() - timeStart < timeout) {
            WorkflowJob workflowJob;
            try {
                workflowJob = oozieClient.getJobInfo(jobId);
            } catch (OozieClientException e) {
                if (reties > MAX_RETRIES) {
                    throw new OozieException(String.format("Failed to get job info after %s retries", reties), e);
                }
                reties++;
                continue;
            }
            reties = 0;
            if (workflowJob.getStatus() == WorkflowJob.Status.FAILED
                    || workflowJob.getStatus() == WorkflowJob.Status.KILLED
                    || workflowJob.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                return;
            }
            try {
                Thread.sleep(CHECK_INTERVAL);
            } catch (InterruptedException e) {
                throw new OozieException(e);
            }
        }
    }

    public WorkflowJob getWorkflowJob() throws OozieException {
        try {
            return oozieClient.getJobInfo(jobId);
        } catch (OozieClientException e) {
            throw new OozieException(e);
        }
    }

}
