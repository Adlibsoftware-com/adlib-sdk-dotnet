using AdlibSDK.V2.Net.Models;
using log4net;
using log4net.Config;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace AdlibSDK.V2.Net.Examples.Simple
{
    class Program
    {
        #region Variables

        static readonly string TAG = "SDKHigh:";
        //  log4net is used for .net,  log4j can be used for Java implementation, log4python for python
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        static CancellationTokenSource _cancelTokenSource = new CancellationTokenSource(); //allows us to shutdown adlib sdk at anytime
        static Settings _settings;
        static SDKHigh _sdkHigh;
        static string _settingsFilePath = Environment.CurrentDirectory + "\\settings.json";

        #endregion

        #region Main Functionality

        /// <summary>
        /// Main entry point of this sample application
        /// </summary>
        /// <param name="args"></param>
        static async Task Main(string[] args)
        {
            //  Initialize a logger - see log4net.config to customize how logging works. currently Console and Log file are enabled
            //  But supports much more! see: https://logging.apache.org/log4net/release/config-examples.html
            Console.WriteLine(TAG + "Main() Initializing logging..");

            try
            {
                XmlDocument log4netConfig = new XmlDocument();
                using (var fs = File.OpenRead("log4net.config"))
                {
                    log4netConfig.Load(fs);
                    var repo = LogManager.CreateRepository(Assembly.GetEntryAssembly(), typeof(log4net.Repository.Hierarchy.Hierarchy));
                    XmlConfigurator.Configure(repo, log4netConfig["log4net"]);
                }

                if (!Directory.Exists(Environment.CurrentDirectory + "\\logs"))
                {
                    Directory.CreateDirectory(Environment.CurrentDirectory + "\\logs");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(TAG + "Main() Exception initializing logger", ex);
                return;
            }

            if (_log != null) _log.Info(TAG + "Main() Logging initialized successfully");

            //  Load settings from config file
            try
            {
                if (_log != null) _log.Info(TAG + "Main() loaded settings");
                _settings = JsonConvert.DeserializeObject<Settings>(System.IO.File.ReadAllText(_settingsFilePath));

                //  TODO add any overrides to your settings file you need here
            }
            catch (Exception ex)
            {
                Console.WriteLine(TAG + "Main() Exception loading settings from path: " + _settingsFilePath, ex);
                return;
            }

            //  Start the SDK
            try
            {
                if (_log != null) _log.Info(TAG + "Main() Staring Adlib SDK...");

                _sdkHigh = new SDKHigh();
                _sdkHigh.CompletedJobEvent += OnCompletedJob;
                _sdkHigh.FailedJobEvent += OnFailedJob;

                //  Creates executors/thread group for submissions and downloads, check connectivity to server, etc.
                if (!_sdkHigh.Init(_log, _cancelTokenSource.Token, _settings))
                {
                    if (_log != null) _log.Error(TAG + "Main() Failed to start SDK, exiting");
                    return;
                }

                //  Fire up tasks to get job completions from Adlib. When there are some OnCompletedJob will fire.
                if (!_sdkHigh.StartCompletionListening())
                    return;
            }
            catch (Exception ex)
            {
                if (_log != null) _log.Error(TAG + "Main() Exception initializing adlib SDK, ex:" + ex.Message);

                //  To avoid shutting down of the app giving user ability to read exception message
                Console.WriteLine("Press any key to shutdown....");
                Console.ReadKey();
                return;
            }

            if (_log != null) _log.Info(TAG + "Main() Adlib SDK Initialized successfully");

            //  Start a task to at intervals check for incoming files and submit them
            await Task.Run(SubmitterWorker, _cancelTokenSource.Token);

            //  Closing application
            Console.WriteLine("SDK shutting down....");

            //  Cleanup resources
            if (_log != null) _log.Debug(TAG + "Main() Shutting down, clearing up resources...");
            _cancelTokenSource.Cancel();
            _sdkHigh.StopCompletionListening();

            //  Event cleanup to prevent memory leaks
            _sdkHigh.CompletedJobEvent -= OnCompletedJob;
            _sdkHigh.FailedJobEvent -= OnFailedJob;
            _sdkHigh.Dispose();

            if (_log != null) _log.Debug(TAG + "Main() Done. Exiting.");
        }

        /// <summary>
        /// Task to check for new files from an input folder
        /// prepare the files for sending, add any custom metadata you want
        /// then send the files to Adlib for processing
        /// A separate event will be fired when they are ready to review the results
        /// </summary>
        private static void SubmitterWorker()
        {
            if (_log != null) _log.Debug(TAG + "SubmitterWorker() Monitoring incoming files");

            //  Start processing files
            while (!_cancelTokenSource.IsCancellationRequested)
            {
                //  Only wait to process files when Merging is enabled(files to one orchestration)
                if (bool.Parse(_settings.SubmitAllFilesToOneOrchestration))
                {
                    Console.WriteLine();
                    Console.WriteLine("Start processing files(Y)? -- To exit, press(E)?:");
                    bool incorrectKey = true;

                    //  Only accept Y or E to prevent file processing by mistake
                    while (incorrectKey)
                    {
                        switch (Console.ReadKey().Key.ToString())
                        {
                            case "Y": incorrectKey = false; break;
                            case "E": return;
                        }                                                
                    }

                    Console.WriteLine();
                }

                try
                {
                    //  Automatically pickup local files from a monitored location,
                    //  Then prepare them with a base payload based on settings
                    var jobs = _sdkHigh.Load();

                    //  Or you can roll your own to trigger whenever you want, it doesn't need to be in a loop here
                    //  var jobs = _sdkHigh.Load(new List<string> { "myfile1", "myfile2" });

                    if (jobs == null || jobs.Count == 0) continue;
                    if (_cancelTokenSource.IsCancellationRequested) break;

                    //  Optional: user can add any custom meta data for this job submission in job.Metadata
                    //  jobs[0].Metadata.AddRange({ new Metadata("ApplyHeader1","True"), new Metadata("ApplyFooter2","True") });

                    //  Also you can add metadata for a specific file such as adding an overlay to a single file
                    //  job[0].Files[0].jobFile.Metadata.Add(new MetadataItem("Overlay._doc1","True"));

                    //  Go ahead and submit the files to JMS for processing
                    //  This will take care of cleaning up files
                    if (!_sdkHigh.Submit(jobs, _cancelTokenSource.Token))
                    {
                        if (_log != null) _log.Error(TAG + "SubmitterWorker() Error submitting files to Adlib");
                        continue;
                    }

                    //  Exit on cancellation request
                    if (_cancelTokenSource.IsCancellationRequested) break;


                    //  Optional: user can check submitted files and get fileid for tracking purposes
                    foreach (var job in jobs)
                    {
                        foreach (var file in job.Files)
                        {
                            if (_log != null) _log.Debug($"Program:SubmitterWorker() jobId={job.JobId}, fileId={file.FileId}, file {file.JobFile.Path}, status={file.Status.ToString()}");
                            //  For example customer updates SAP for this file with fileid as submitted
                        }
                    }

                    //_log.Debug(TAG + "SubmitterWorker() - Processing files........ ");
                    //  An async event will fire when the jobs are complete
                }
                catch (OperationCanceledException)
                {
                    return; //  CancellationToken called, exit cleanly
                }
                catch (Exception ex)
                {
                    if (_log != null) _log.Error(TAG + "SubmitterWorker() Exception processing files", ex);
                }
                finally
                {
                    //  Delay to avoid high cpu
                    Thread.Sleep(Convert.ToInt32(_settings.InputFileCheckIntervalInSec) * 1000);
                }
            }

            if (_log != null) _log.Debug(TAG + "SubmitterWorker() Task exited");
        }

        #endregion

        #region Events

        /// <summary>
        /// Event that gets fired when completed files have been downloaded
        /// Customer would possibly what to check statuses and Ids and update in their own system
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void OnCompletedJob(object sender, EventArgs e)
        {
            try
            {
                //  Files have already been cleaned up on the adlib side
                //  Don't exit if cancelled, allow events of partially transferred to come in so we have
                //  A complete transfer and recording and release of resources locally and on server!
                //  if (_cancelTokenSource.IsCancellationRequested) return;

                Job job = ((CompletedJobEventArgs)e).Job;
                if (job == null || job.Files.Count == 0)
                {
                    if (_log != null) _log.Error(TAG + "OnCompletedJob() Event has a null Job");
                    return;
                }

                //  Optional: user can check the resulting file and do something with it, including meta data
                foreach (var file in job.Files)
                {
                    if (_log != null) _log.Debug(TAG + $"OnCompletedJob() file {file.JobFile.Path}, status={file.Status.ToString()}, fileId={file.FileId}");
                    //  TODO update/notify your company's systems that this job succeeded and completed.
                }
            }
            catch (Exception ex)
            {
                if (_log != null) _log.Error(TAG + "OnCompletedJob() Exception ", ex);
            }
        }

        /// <summary>
        /// When a failure downloading from JMS occurs this event is fired
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void OnFailedJob(object sender, EventArgs e)
        {
            //  Don't exit if cancelled, allow events of partially transferred to come in so we have
            //  A complete transfer and recording and release of resources locally and on server!
            //  if (_cancelTokenSource.IsCancellationRequested) return;

            string msg = ((FailedJobEventArgs)e).ErrorMessage;
            Job job = ((FailedJobEventArgs)e).Job;

            if (job == null || job.Files.Count == 0)
            {
                if (_log != null) _log.Error(TAG + "OnFailedJob() unknown Job failed with message: " + msg);
                return;
            }

            //  Optional: user can check the resulting file and do something with it, including meta data
            foreach (var file in job.Files)
            {
                if (_log != null) _log.Error(TAG + $"OnFailedJob() jobid:{job.JobId}, path:{file.JobFile.Path}, failed with message={msg}");
                //  TODO update/notify your company's systems that this job failed
            }
        }

        #endregion
    }
}
