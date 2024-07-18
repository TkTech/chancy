import React, { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query';
import { useForm, Field } from '@tanstack/react-form';

const FormField = ({ field, formField }) => (
  <div className="mb-3">
    <label htmlFor={field.name} className="form-label">{field.label}:</label>
    {field.type === "json" ? (
      <textarea
        id={field.name}
        name={field.name}
        value={formField.state.value}
        className="form-control"
        readOnly={field.readonly}
        required={field.required}
        onBlur={formField.handleBlur}
        onChange={(e) => formField.handleChange(e.target.value)}
      />
    ) : (
      <input
        id={field.name}
        name={field.name}
        type={field.type}
        value={formField.state.value}
        className="form-control"
        readOnly={field.readonly}
        required={field.required}
        onBlur={formField.handleBlur}
        onChange={(e) => formField.handleChange(e.target.value)}
      />
    )}
    {field.description && (
      <div className="form-text">{field.description}</div>
    )}
  </div>
);

export function Queue() {
  const { queueName } = useParams();
  const queryClient = useQueryClient();

  const { data: workers, isLoading: workersIsLoading } = useQuery({
    queryKey: ['workers'],
    queryFn: async () => {
      const response = await fetch('/api/workers');
      return response.json();
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData
  });

  const { data: queue, isLoading } = useQuery({
    queryKey: ['queue', queueName],
    queryFn: async () => {
      const response = await fetch(`/api/queues/${queueName}`);
      // Merge the workers into the response
      const queueData = await response.json();
      if (workers) {
        queueData.workers = workers.filter((worker) => worker.queues.includes(queueName));
      }
      return queueData;
    },
    enabled: !!workers,
  });

  const fields = [
    {
      name: "name",
      label: "Queue Name",
      type: "text",
      readonly: true,
      description: "The globally unique name of the queue."
    },
    {
      name: "concurrency",
      label: "Concurrency",
      type: "number",
      required: true,
      description: "The number of jobs that can be processed concurrently on each worker.",
      default: 5
    },
    {
      name: "polling_interval",
      label: "Polling Interval",
      type: "number",
      required: true,
      description: "The number of seconds between polling the queue for new jobs.",
      default: 1
    },
    {
      name: "executor",
      label: "Executor",
      type: "text",
      readonly: true,
      description: "The executor used to process jobs in this queue.",
      default: "chancy.executors.process.ProcessExecutor"
    },
    {
      name: "executor_options",
      label: "Executor Options",
      type: "json",
      description: "Additional options to pass to the executor.",
      default: "{}"
    }
  ];

  const form = useForm({
    defaultValues: fields.reduce((acc, field) => {
      acc[field.name] = field.default || "";

      if (queue && field.name in queue) {
        acc[field.name] = queue[field.name];
        if (field.type === "json") {
          acc[field.name] = JSON.stringify(queue[field.name], null, 2);
        }
      }
      return acc;
    }),
    onSubmit: async ({ values }) => {
      console.log(values);
    },
  })

  if (isLoading || workersIsLoading) {
    return (
      <div className="d-flex justify-content-center">
        <div className="spinner-border" role="status">
          <span className="visually-hidden">Loading...</span>
        </div>
      </div>
    );
  }

  return (
    <>
      <div className={"row"}>
        <div className={"col"}>
          <div className={"alert alert-info"} role="alert">
            Editing of queues using the dashboard is not yet supported - please use the API.
          </div>
        </div>
      </div>
      <div className={"row"}>
        <div className={"col"}>
          <h1 className={"h5"}>General Settings</h1>
          <hr />
          <form
            onSubmit={(e) => {
              e.preventDefault();
              e.stopPropagation();
              form.handleSubmit();
            }}
          >
            {fields.map((field) => (
              <form.Field
                key={field.name}
                name={field.name}
                children={(formField) => (
                  <FormField field={field} formField={formField} />
                )}
              />
            ))}
          </form>
        </div>
      </div>
      <div className={"row"}>
        <div className={'col'}>
          <h2 className={'h5'}>Tags</h2>
          <p className={'text-muted'}>This queue will run on any worker which matches these regex.</p>
          <hr/>
          <ul className={'list-inline'}>
            {queue.tags.map((tag) => (
              <li className={'list-inline-item'} key={tag}>
                <span className={'badge text-bg-secondary'}>{tag}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
      <div className={'row'}>
        <div className={'col'}>
          <h2 className={'h5'}>Workers</h2>
          <p className={"text-muted"}>Workers that are currently processing jobs from this queue.</p>
          <hr />
          <table className="table table-striped table-bordered mt-3">
            <thead>
              <tr>
                <th>ID</th>
                <th>Worker Tags</th>
              </tr>
            </thead>
            <tbody>
              {queue.workers.map((worker) => (
                <tr key={worker.worker_id}>
                  <td>{worker.worker_id}</td>
                  <td>
                    {worker.tags.sort().map((tag: string) => (
                      <li className={"list-inline-item"} key={tag}>
                        <span className={"badge text-bg-secondary"}>{tag}</span>
                      </li>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}