package kr.excorp.batch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;


public class SimpleJobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("SimpleJobListener beforeJob");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("SimpleJobListener afterJob");
    }
}
