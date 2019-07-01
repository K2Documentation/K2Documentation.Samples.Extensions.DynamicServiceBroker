using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
//we'll use the System.Data namespace to read a XML file into a dataset
using System.Data;
//if you want to implement transaction support, import the System.Transactions namespace
//and implement the Transaction support methods
using System.Transactions;

using SourceCode.SmartObjects.Services.ServiceSDK;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;

namespace K2Documentation.Samples.Extensions.DynamicServiceBroker.XML
{
    class CustomXMLServiceBroker : ServiceAssemblyBase
    {
        #region Default Constructor
        /// <summary>
        /// Instantiates a new ServiceBroker.
        /// </summary>
        public CustomXMLServiceBroker()
        {
            // No implementation necessary.
        }
        #endregion

        #region Service Broker Implementation Methods (Override base class methods)

        #region override string GetConfigSection()
        /// <summary>
        /// Sets up the required configuration items (and default values) for a Service Instance of the Service Broker. 
        /// When a new service instance is registered for this ServiceBroker, the configuration parameters are surfaced to the UI. 
        /// The configuration values are usually provided by the person registering the service instance.
        /// you can set default values for the configuration settings and indicate whether the setting is required or optional
        /// </summary>
        /// <returns>A string containing the configuration XML.</returns>
        public override string GetConfigSection()
        {
            try
            {
                //for this particular Broker, add a single, required configuration value which accepts the path of the XML file we want to expose
                this.Service.ServiceConfiguration.Add("XMLFilePath", true, string.Empty);
                //you can add more configuration settings by repeating
                //this.Service.ServiceConfiguration.Add("SettingName", true, string.Empty);
            }
            catch (Exception ex)
            {
                // Record the exception message and indicate that this was an error.
                ServicePackage.ServiceMessages.Add(ex.Message, MessageSeverity.Error);
            }

            //return the configuration section
            return base.GetConfigSection();
        }
        #endregion

        #region override string DescribeSchema()
        /// <summary>
        /// Describes the schema of the underlying data and services as equivalent SmartObject types
        /// This method is called whenever a service instance is registered or a service instance
        /// is refreshed
        /// </summary>
        /// <returns>A string containing the schema XML. The string is returned by executing the base.DescribeSchema() method
        /// after adding ServiceObjects to the this.Service.ServiceObjects collection</returns>
        public override string DescribeSchema()
        {
            try
            {
                //1. GET SERVICE INSTANCE CONFIGURATION VALUES
                //we need to retrieve the Service Instance configuration settings before calling the DescribeSchema method, since
                //the specific configuration value we're retrieving will tell us where to find the XML file that we want to discover
                string xmlFilePath = this.Service.ServiceConfiguration["XMLFilePath"].ToString();


                //2. SET UP SERVICE OBJECT TYPE MAPPINGS
                //set up the Service Object DataType Mappings for the service. This is a table which tells the service discovery method
                //how to map the native data types for the Provider to equivalent Service Object data types
                TypeMappings map = new TypeMappings();
                // Add type mappings.
                map.Add("Int32", SoType.Number);
                map.Add("String", SoType.Text);
                map.Add("Boolean", SoType.YesNo);
                map.Add("Date", SoType.DateTime);
                // Add the type mappings to the Service Instance.
                this.Service.ServiceConfiguration.Add("Type Mappings", map);

                //3. DISCOVER THE SCHEMA OF THE UNDERLYING PROVIDER
                //here we will connect to the Provider and discover the schema. During the discovery phase, we will
                //add one or more ServiceObjects (SourceCode.SmartObjects.Services.ServiceSDK.Objects) to the Service Instance. 
                //Each ServiceObject contains a collection of Properties of type SourceCode.SmartObjects.Services.ServiceSDK.Objects.Property
                //and a collection of Methods of type SourceCode.SmartObjects.Services.ServiceSDK.Objects.Method
                //see the DiscoverXMLFileSchema method for an example of iterating over the objects in the Provider and adding service objects
                DiscoverXMLFileSchema(xmlFilePath);

                //4. SET UP THE SERVICE INSTANCE (Optional)
                //Set up the default values for the Service Instance. The user will be able to override these values
                this.Service.Name = "K2LearningDynamicXMLService";
                this.Service.MetaData.DisplayName = "K2Learning Custom Service Broker (Discovered Schema)";
                this.Service.MetaData.Description = "This custom Service discovers a XML file and returns the items in the XML file as Service Objects";

                // Indicate that the operation was successful.
                ServicePackage.IsSuccessful = true;
            }
            catch (Exception ex)
            {
                // Record the exception message and indicate that this was an error.
                ServicePackage.ServiceMessages.Add(ex.Message, MessageSeverity.Error);
                // Indicate that the operation was unsuccessful.
                ServicePackage.IsSuccessful = false;
            }

            return base.DescribeSchema();
        }
        #endregion
      
        #region override void Execute()
        /// <summary>
        /// Executes a Service Object method at runtime and returns any data.
        /// This is the entry point for any execution of any Service Object, You will need to determine which method was 
        /// requested along with the input properties and parameters, and then call an appropriate helper method to query the Provider
        /// </summary>
        public override void Execute()
        {
            try
            {
                //1. GET THE SERVICE INSTANCE CONFIGURATION
                string xmlFilePath = this.Service.ServiceConfiguration["XMLFilePath"].ToString();

                //2. GET THE SERVICE OBJECT THAT WAS REQUESTED
                //at runtime, the requested Service Object is always in the [0] position of the Service.ServiceObjects array
                ServiceObject serviceObject = Service.ServiceObjects[0];

                //3. GET THE METHOD THAT WAS REQUESTED
                //at runtime, the requested Service Object Method is always in the [0] position of the ServiceObject.Methods array
                Method method = serviceObject.Methods[0];

                //4. GET THE INPUT PROPERTIES AND RETURN PROPERTIES FOR THE REQUESTED METHOD
                // InputProperties and ReturnProperties are string collections, create property collections for later ease-of-use.
                Property[] inputs = new Property[method.InputProperties.Count];
                Property[] returns = new Property[method.ReturnProperties.Count];
                MethodParameter[] parameters = new MethodParameter[method.MethodParameters.Count];
                
                //populate the Input Properties collection
                for (int i = 0; i < method.InputProperties.Count; i++)
                {
                    inputs[i] = serviceObject.Properties[method.InputProperties[i]];
                }

                //populate the return properties collection
                for (int i = 0; i < method.ReturnProperties.Count; i++)
                {
                    returns[i] = serviceObject.Properties[method.ReturnProperties[i]];
                }

                //populate the method parameters collection
                for (int i = 0; i < method.MethodParameters.Count; i++)
                {
                    parameters[i] = method.MethodParameters[i];
                }
                
                //OBTAINING THE SECURITY CREDENTIALS FOR A SERVICE INSTANCE
                //if you need to obtain the authentication credentials (username/password) for the service instance, query the following properties:
                //Note: password may be blank unless you are using Static or SSO credentials
                string username = this.Service.ServiceConfiguration.ServiceAuthentication.UserName;
                string password = this.Service.ServiceConfiguration.ServiceAuthentication.Password;


                //5. EXECUTE THE ACTUAL METHOD
                //here we will call a helper method to do the actual execution of the method. 
                ExecuteAgainstXMLFile(inputs, returns, method.Type, serviceObject, xmlFilePath);

                // Indicate that the operation was successful.
                ServicePackage.IsSuccessful = true;
            }
            catch (Exception ex)
            {
                // Record the exception message and indicate that this was an error.
                ServicePackage.ServiceMessages.Add(ex.Message, MessageSeverity.Error);
                // Indicate that the operation was unsuccessful.
                ServicePackage.IsSuccessful = false;
            }
        }
        #endregion

        #region override void Extend()
        /// <summary>
        /// Extends the underlying system or technology's schema. This is only implemented for K2 SmartBox.
        /// </summary>
        public override void Extend()
        {
            try
            {
                throw new NotImplementedException("Service Object \"Extend()\" is not implemented.");
            }
            catch (Exception ex)
            {
                // Record the exception message and indicate that this was an error.
                ServicePackage.ServiceMessages.Add(ex.Message, MessageSeverity.Error);
                // Indicate that the operation was unsuccessful.
                ServicePackage.IsSuccessful = false;
            }
        }
        #endregion

        #endregion

        #region Transaction Support Methods

        #region void Commit(Enlistment enlistment)
        /// <summary>
        /// Responds to the Commit notification.
        /// </summary>
        /// <param name="enlistment">An Enlistment that facilitates communication between the enlisted transaction participant and the transaction manager during the final phase of the transaction.</param>
        public override void Commit(Enlistment enlistment)
        {
            if (enlistment != null)
            {
                // Indicate that the transaction participant has completed its work.
                enlistment.Done();
            }
        }
        #endregion

        #region void InDoubt(Enlistment enlistment)
        /// <summary>
        /// Responds to the InDoubt notification.
        /// </summary>
        /// <param name="enlistment">An Enlistment that facilitates communication between the enlisted transaction participant and the transaction manager during the final phase of the transaction.</param>
        public override void InDoubt(Enlistment enlistment)
        {
            if (enlistment != null)
            {
                // Indicate that the transaction participant has completed its work.
                enlistment.Done();
            }
        }
        #endregion

        #region void Prepare(PreparingEnlistment preparingEnlistment)
        /// <summary>
        /// Responds to the Prepare notification.
        /// </summary>
        /// <param name="preparingEnlistment">An Enlistment that facilitates communication between the enlisted transaction participant and the transaction manager during the Prepare phase of the transaction.</param>
        public override void Prepare(PreparingEnlistment preparingEnlistment)
        {
            // Allow the base class to handle the Prepare notification.
            base.Prepare(preparingEnlistment);
        }
        #endregion

        #region void Rollback(Enlistment enlistment)
        /// <summary>
        /// Responds to the Rollback notification.
        /// </summary>
        /// <param name="enlistment">An Enlistment that facilitates communication between the enlisted transaction participant and the transaction manager during the final phase of the transaction.</param>
        public override void Rollback(Enlistment enlistment)
        {
            if (enlistment != null)
            {
                //do the necessary work to roll back the transaction
                // Indicate that the transaction participant has completed its work.
                enlistment.Done();
            }
        }
        #endregion

        #endregion

        #region Helper Methods
        /// <summary>
        /// This method is specifically written to discover the schema of a XML file
        /// we are only catering for a simple datatable where the XML file contains a single repeating structure
        /// We will load the XML file into a dataSet, extract DataTables from the dataset and then create
        /// Service Objects for each data table. We will then create Properties for each column in the data set, and finally
        /// create two Methods (List and Read) for each Service Object.
        /// </summary>
        private void DiscoverXMLFileSchema(string xmlFilePath)
        {
            //read the type mappings so that we can convert the DataTable's property types into 
            //equivalent Service Object Property types
            TypeMappings map = (TypeMappings)this.Service.ServiceConfiguration["Type Mappings"];
            
            //load the XML file specified in the service instance configuration into a dataset so that we can discover it
            try
            {
                //read the target XML file into a dataset so we can discover it
                DataSet pseudoDataSource = new DataSet("PseudoDataSource");
                pseudoDataSource.ReadXml(xmlFilePath);

                //iterate through each DataTable in the DataSource
                foreach (DataTable table in pseudoDataSource.Tables)
                {
                    //1. CREATE SERVICE OBJECTS
                    //we will create a Service Object for each table in the DataSet
                    ServiceObject svcObject = new ServiceObject();
                    //clean up the System Name
                    svcObject.Name = table.TableName.Replace(" ", "");
                    svcObject.MetaData.DisplayName = table.TableName;

                    //2. CREATE SERVICE OBJECT PROPERTIES
                    //we will create Service Object Properties for each column in the Table
                    foreach (DataColumn column in table.Columns)
                    {
                        //note that the Name cannot have spaces
                        Property svcProperty = new Property(column.ColumnName.Replace(" ",""));
                        svcProperty.MetaData.DisplayName = column.ColumnName;
                        //set the property type based on the type mappings defined for the service
                        svcProperty.SoType = map[column.DataType.Name];
                        //svcObject.Properties.Add(svcProperty);
                        svcObject.Properties.Create(svcProperty);
                    }

                    //3. CREATE SERVICE OBJECT METHODS
                    //we will only create Read and List methods for the XML Service Broker 
                    
                    //LIST Method.
                    Method svcListMethod = new Method();
                    //note that the Name should not contain spaces
                    svcListMethod.Name = "List" + table.TableName.Replace(" ", "");
                    svcListMethod.MetaData.DisplayName = "List " + table.TableName;
                    svcListMethod.Type = MethodType.List;
                    //Set up the return properties for the List Method
                    ReturnProperties listReturnProperties = new ReturnProperties();
                    //for this method we'll return each column as a property. 
                    foreach (Property svcProperty in svcObject.Properties)
                    {
                        listReturnProperties.Add(svcProperty);
                    }
                    svcListMethod.ReturnProperties = listReturnProperties;

                    //Set up the input properties for the List Method
                    InputProperties listInputProperties = new InputProperties();
                    //for this method we'll return each column as a property. 
                    foreach (Property svcProperty in svcObject.Properties)
                    {
                        listInputProperties.Add(svcProperty);
                    }
                    svcListMethod.InputProperties = listInputProperties;

                    //call the create factory to add the service object method
                    svcObject.Methods.Create(svcListMethod);

                    //READ Method
                    Method svcReadMethod = new Method();
                    //note that the Name should not contain spaces
                    svcReadMethod.Name = "Read" + table.TableName.Replace(" ", "");
                    svcReadMethod.MetaData.DisplayName = "Read " + table.TableName;
                    svcReadMethod.Type = MethodType.Read;

                    
                    //Set up the return properties for the Read Method
                    ReturnProperties readReturnProperties = new ReturnProperties();
                    //for this method we will return each column as a property. 
                    foreach (Property svcProperty in svcObject.Properties)
                    {
                        readReturnProperties.Add(svcProperty);
                    }
                    svcReadMethod.ReturnProperties = readReturnProperties;

                    //Set up the input properties for the method
                    InputProperties inputProperties = new InputProperties();
                    //for this method we will define the first column in the data table as the input property
                    inputProperties.Add(svcObject.Properties[0]);
                    svcReadMethod.InputProperties = inputProperties;

                    //define the required properties for the method. 
                    //in this case, we will assume that the first property of the item is also the Key value for the item
                    //which we will require to locate the specified item in the XML file
                    svcReadMethod.Validation.RequiredProperties.Add(inputProperties[0]);

                    //add the Read method to the Service Object using the create factory
                    svcObject.Methods.Create(svcReadMethod);

                    //Use Method parameters if you want to define a parameter for the method
                    //Parameters are used when the required input value is not already a Property of the Service Object. 
                    //if the required input value is already defined as a property for the SmartObject, use RequiredProperties instead
                    //MethodParameter readMethodParameter = new MethodParameter();
                    //readMethodParameter.Name = "SomeName";
                    //readMethodParameter.MetaData.DisplayName = "SomeDisplayName";
                    //readMethodParameter.SoType = one of the available SO Types];
                    //svcReadMethod.MethodParameters.Add(readMethodParameter);

                    //4. ADD THE SERVICE OBJECT TO THE SERVICE INSTANCE
                    // Activate Service Object for use, otherwise you cannot create SmartObjects for the service object
                    svcObject.Active = true;
                    //add the Service Object to the Service Type, using the create factory
                    this.Service.ServiceObjects.Create(svcObject);
                }
            }

            catch (Exception ex)
            {
                throw new Exception("Error while attempting to DiscoverXMLFileSchema. Error: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Helper method to execute the requested Method against the Provider. 
        /// </summary>
        /// <param name="inputs">A Property[] array containing all the defined input properties. Properties entered by the user will have a value.
        /// Properties not entered by the user will have a null value</param>
        /// <param name="returns">A Property[] array containing all the defined return properties for the requested method</param>
        /// <param name="methodType">A MethodType indicating what type of Service Object method was requested.</param>
        /// <param name="serviceObject">A ServiceObject definition containing populated properties for use with the method call.</param>
        /// <param name="xmlFilePath">The path to the XML file as retrieved from the service instance configuration</param>
        public void ExecuteAgainstXMLFile(Property[] inputs, Property[] returns, MethodType methodType, ServiceObject serviceObject, string xmlFilePath)
        {
            //we'll just load the target XML file into a data table then generate and return Service Objects for each item in the XML file
            //for the requested service object type (table name)
            DataSet pseudoDataSource = new DataSet("PseudoDataSource");
            pseudoDataSource.ReadXml(xmlFilePath);
            //the Service Object name is the table name we want to retrieve
            DataTable table = pseudoDataSource.Tables[serviceObject.Name];

            //1. CHECK WHAT TYPE OF METHOD WAS REQUESTED
            //List Method
            if (methodType == MethodType.List)
            {
                //2. PREPARE A RESULT TABLE TO HOLD THE RECORDS
                serviceObject.Properties.InitResultTable();

                ////if you want to implement transaction handling, create the transaction scope and enlist the transaction
                ////before performing a volatile operation
                //Transaction current = Transaction.Current;
                //if (current != null)
                //{
                //    current.EnlistVolatile(this.Service.Parent, EnlistmentOptions.None);
                //}
                

                //In this Service Broker, we will filter the results based on the input properties for the method
                //for performance, it is a good idea to filter data as close to the source as possible but this may not be 
                //possible. If you do not define Input Properties, you can always filter the data "client side" on the K2 server rather than on the Provider.
                string concatOperator = "";
                StringBuilder filterExpressionBuilder = new StringBuilder();
                //loop over each input property for the 
                foreach (Property inputProperty in inputs)
                {
                    if (inputProperty.Value != null && !string.IsNullOrEmpty(inputProperty.Value.ToString()))
                    {
                        filterExpressionBuilder.Append(concatOperator + inputProperty.Name + " LIKE '" + inputProperty.Value.ToString() + "%'");
                        concatOperator = " AND ";
                    }
                }

                //2. ITERATE OVER THE COLLECTION OF ITEMS AND INSERT RECORDS INTO THE RESULT TABLE
                foreach (DataRow row in table.Select(filterExpressionBuilder.ToString()))
                {
                    //4. POPULATE THE PROPERTIES FOR THE RECORD
                    for (int i = 0; i < returns.Length; i++)
                    {
                        serviceObject.Properties[i].Value = row.ItemArray[i];
                    }
                    //4. ADD THE RECORD TO THE RESULT TABLE
                    serviceObject.Properties.BindPropertiesToResultTable();
                }
                return;
            }

            //Read Method
            if (methodType == MethodType.Read)
            {
                // Prepare the Service Object to receive returned data.
                serviceObject.Properties.InitResultTable();

                //return a single item. 
                //In the real world, you should use filtering on the provider side wherever possible to reduce network traffic and improve performance
                foreach (DataRow row in table.Select(inputs[0].Name+ " = '" + inputs[0].Value.ToString().Trim() + "'"))
                {
                    // Set Service Object data.
                    for (int i = 0; i < returns.Length; i++)
                    {
                        serviceObject.Properties[i].Value = row.ItemArray[i];
                    }
                    // Commit the changes to the Service Object.
                    serviceObject.Properties.BindPropertiesToResultTable();
                    return;
                }
            }
        }
        #endregion
    }
}
