package kr.excorp.batch;

import kr.excorp.batch.listener.SimpleChunkListener;
import kr.excorp.batch.listener.SimpleJobListener;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@RequiredArgsConstructor
public class SimpleJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job simpleTaskletJob() {
        //simple tasklet 0
        //simple tasklet 1
        //simple tasklet 2
        //simple tasklet 3
        //simple tasklet 4
        //simple tasklet 5
        return jobBuilderFactory.get("simpleTaskletJob")
                .start(simpleTaskletStep())
                .build();
    }

    @Bean
    @JobScope
    public Step simpleTaskletStep() {
        return stepBuilderFactory.get("stepBuilderFactory")
                .tasklet(new Tasklet() {
                    int i = 0;
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("simple tasklet " + i++);
                        return i > 5? RepeatStatus.FINISHED : RepeatStatus.CONTINUABLE;
                    }
                })
                .build();
    }


    //////////


    @Bean
    public Job parameterJob() {
        //name parameter testing
        return jobBuilderFactory.get("parameterJob")
                .start(parameterStep(null))  //null로 하면 알아서 넣어줌
                .build();
    }

    @Bean
    @JobScope
    public Step parameterStep(@Value("#{jobParameters['name']}") String name) {
        return stepBuilderFactory.get("parameterStep")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("name parameter " + name);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }


    //////////


    @Bean
    public Job readerProcessorWriterJob() {
        //SimpleJobListener beforeJob
        //SimpleChunkListener beforeStep
        //reader 1
        //reader 2
        //reader 3
        //reader 4
        //reader 5
        //processor 1
        //processor 2
        //processor 3
        //processor 4
        //processor 5
        //writer 2, 4, 6, 8, 10
        //reader 6
        //reader 7
        //reader 8
        //reader 9
        //reader 10
        //processor 6
        //processor 7
        //processor 8
        //processor 9
        //processor 10
        //writer 12, 14, 16, 18, 20
        //SimpleChunkListener afterStep
        //SimpleJobListener afterJob
        return jobBuilderFactory.get("readerProcessorWriterJob")
                .start(readerProcessorWriterStep())
                .listener(new SimpleJobListener())
                .build();
    }

    @Bean
    @JobScope
    public Step readerProcessorWriterStep() {
        return stepBuilderFactory.get("readerProcessorWriterStep")
                .<Integer, String>chunk(5)
                .reader(new ItemReader<>() {
                    int i = 0;
                    @Override
                    public Integer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                        i++;
                        if (i <= 10) {
                            System.out.println("reader " + i);
                            return i;
                        }
                        return null;
                    }
                })
                .processor((ItemProcessor<Integer, String>) integer -> {
                    System.out.println("processor " + integer);
                    return String.valueOf(integer * 2);
                })
                .writer((list) -> {
                    System.out.println("writer " + String.join(", ", list));
                })
                .listener(new SimpleChunkListener())
                .build();
    }
}
