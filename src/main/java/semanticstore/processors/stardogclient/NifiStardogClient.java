package semanticstore.processors.stardogclient;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.openrdf.rio.RDFFormat;

import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;

/**
 * @author Charbull
 * */

@Tags({"StarDog", "Triples insertion", "rdf"})
@CapabilityDescription("SNARL client insert RDF into a Stardog")
@SeeAlso({})
@ReadsAttributes({@org.apache.nifi.annotation.behavior.ReadsAttribute(attribute="InputStream", description="RDF Input Stream")})
public class NifiStardogClient
extends AbstractProcessor
{
	private static final String USER_NAME = "admin";
	private static final String PASSWORD = "admin";
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	public static final PropertyDescriptor STARDOG_ENPOINT = new PropertyDescriptor.Builder().name("Stardog Endpoint And Port").description("The Server URL where StarDog is running, dev-semanticstore.cloudapp.net:5820").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder().name("DataBase Name").description("The DB Name where the triples are inserted").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();


	public static final PropertyDescriptor USER_NAME_Property = new PropertyDescriptor.
			Builder().name("Username").required(false).
			addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor PASSWORD_Property = new PropertyDescriptor.Builder().name("Password")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").description("Succes relationship").build();

	protected void init(ProcessorInitializationContext context)
	{
		List<PropertyDescriptor> descriptors = new ArrayList();
		descriptors.add(STARDOG_ENPOINT);
		descriptors.add(DB_NAME);
		descriptors.add(USER_NAME_Property);
		descriptors.add(PASSWORD_Property);

		this.descriptors = Collections.unmodifiableList(descriptors);

		Set<Relationship> relationships = new HashSet();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
	{
		return this.descriptors;
	}

	public Set<Relationship> getRelationships()
	{
		return this.relationships;
	}

	@OnScheduled
	public void onScheduled(ProcessContext context) {}

	public void onTrigger(ProcessContext context, ProcessSession session)
			throws ProcessException
	{
		FlowFile flowfile = session.get();

		final String stardog_endpoint = context.getProperty(STARDOG_ENPOINT).getValue();
		final String db_name = context.getProperty(DB_NAME).getValue();

		final String username = ((context.getProperty(USER_NAME_Property).getValue() != null)?
				context.getProperty(USER_NAME_Property).getValue() : USER_NAME); 

		final String password = ((context.getProperty(PASSWORD_Property).getValue() != null)?
				context.getProperty(PASSWORD_Property).getValue() : PASSWORD); 


		session.read(flowfile, new InputStreamCallback()
		{
			public void process(InputStream inputStream)
					throws IOException
			{
				if (inputStream != null) {
					try
					{
						insertData(stardog_endpoint, db_name, username, password, inputStream);
					}
					catch (ProcessException ex)
					{
						ex.printStackTrace();
					}
				}
			}
		});
		session.transfer(flowfile, SUCCESS);
	}

	public static void insertData(String endpoint_port, String dB_name, String userName, String password, InputStream inputStream)
	{
		AdminConnection aAdminConnection = AdminConnectionConfiguration.toServer("http://" + endpoint_port).credentials(userName, password).connect();
		if (!aAdminConnection.list().contains(dB_name)) {
			aAdminConnection.createMemory(dB_name.toString());
			System.out.println("DB Created !");
		}
		
		Connection aConn = ConnectionConfiguration.from("snarl://" + endpoint_port +"/" +dB_name).credentials(userName, password).connect();

		aConn.begin();

		aConn.add().io().format(RDFFormat.RDFXML).stream(inputStream);
		aConn.commit();
		System.out.println("Data Added !");
	}
}
