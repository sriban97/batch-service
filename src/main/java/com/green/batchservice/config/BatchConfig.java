package com.green.batchservice.config;

import com.green.batchservice.batchProcessor.ClientItemProcessor;
import com.green.batchservice.entity.Client;
import com.green.batchservice.repository.ClientRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private ClientRepository clientRepository;

    @Autowired
    private ClientItemProcessor clientItemProcessor;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Bean("lineMapper")
    public LineMapper<Client> getLineMapper() {
        DefaultLineMapper<Client> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(",");
        delimitedLineTokenizer.setStrict(false);
        delimitedLineTokenizer.setNames("id", "firstName", "cli_last_name", "cli_city", "cli_zip_code", "cli_phone_num");

        BeanWrapperFieldSetMapper<Client> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Client.class);

        lineMapper.setLineTokenizer(delimitedLineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }


    @Bean("flatFileItemReader")
    public FlatFileItemReader<Client> getFlatFileItemReader(@Qualifier("lineMapper") LineMapper<Client> lineMapper) {

        FlatFileItemReader<Client> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setName("Client Reader");
        flatFileItemReader.setResource(new FileSystemResource("/opt/workspace/doc/kafka-data.ods"));

        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper);
        return flatFileItemReader;
    }

    @Bean("repositoryItemWriter")
    public RepositoryItemWriter<Client> getRepositoryItemWriter() {
        RepositoryItemWriter<Client> repositoryItemWriter = new RepositoryItemWriter<>();
        repositoryItemWriter.setRepository(clientRepository);
        repositoryItemWriter.setMethodName("save");
        return repositoryItemWriter;
    }

    @Bean("taskExecutor")
    public TaskExecutor getTaskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(1000);
        return asyncTaskExecutor;
    }

    @Bean("step")
    public Step getStep(@Qualifier("flatFileItemReader") FlatFileItemReader<Client> flatFileItemReader
            , @Qualifier("repositoryItemWriter") RepositoryItemWriter<Client> repositoryItemWriter
            , @Qualifier("taskExecutor") TaskExecutor taskExecutor
    ) {
        return new StepBuilder("Client Batch", jobRepository)
                .<Client, Client>chunk(10, platformTransactionManager)
                .reader(flatFileItemReader)
                .processor(clientItemProcessor)
                .writer(repositoryItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean("job")
    public Job getJob(@Qualifier("step") Step step) {
        return new JobBuilder("job", jobRepository).flow(step).end().build();
    }
}
