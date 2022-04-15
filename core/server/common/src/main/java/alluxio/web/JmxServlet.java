/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Set;

/**
 * Provides Read only web access to JMX.
 * <p>
 * This servlet generally will be placed under the /jmx URL for each
 * HttpServer.  It provides read only
 * access to JMX metrics.  The optional <code>qry</code> parameter
 * may be used to query only a subset of the JMX Beans.  This query
 * functionality is provided through the
 * {@link MBeanServer#queryNames(ObjectName, javax.management.QueryExp)}
 * method.
 * <p>
 * For example <code>http://.../metrics/jmx?qry=org.alluxio:*</code> will return
 * all alluxio metrics exposed through JMX.
 *
 */
public class JmxServlet extends HttpServlet {
  private static final Logger LOG =
      LoggerFactory.getLogger(JmxServlet.class);
  static final String ACCESS_CONTROL_ALLOW_METHODS =
      "Access-Control-Allow-Methods";
  static final String ACCESS_CONTROL_ALLOW_ORIGIN =
      "Access-Control-Allow-Origin";

  private static final long serialVersionUID = -8580347928686342051L;

  /**
   * MBean server.
   */
  protected MBeanServer mBeanServer;

  /**
   * Json Factory to create Json generators for write objects in json format.
   */
  protected JsonFactory mJsonFactory;

  /**
   * Initialize this servlet.
   */
  @Override
  public void init() throws ServletException {
    // Retrieve the MBean server
    mBeanServer = ManagementFactory.getPlatformMBeanServer();
    mJsonFactory = new JsonFactory();
  }

  /**
   * Disable TRACE method to avoid TRACE vulnerability.
   */
  @Override
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
  }

  /**
   * Process a GET request for the specified resource.
   *
   * @param request The servlet request we are processing
   * @param response The servlet response we are creating
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    try (PrintWriter writer = response.getWriter();
         JsonGenerator jg = mJsonFactory.createGenerator(writer)) {
      response.setContentType("application/json; charset=utf8");
      response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
      response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");

      jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      jg.useDefaultPrettyPrinter();
      jg.writeStartObject();
      // Query per mbean attribute
      String getmethod = request.getParameter("get");
      if (getmethod != null) {
        String[] splitStrings = getmethod.split("\\:\\:");
        if (splitStrings.length != 2) {
          jg.writeStringField("result", "ERROR");
          jg.writeStringField("message", "query format is not as expected.");
          jg.flush();
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        listBeans(jg, new ObjectName(splitStrings[0]), splitStrings[1],
            response);
        return;
      }

      // Query per mbean
      String qry = request.getParameter("qry");
      if (qry == null) {
        qry = "*:*";
      }
      listBeans(jg, new ObjectName(qry), null, response);
    } catch (IOException e) {
      LOG.error("Caught an exception while processing JMX request: {}", request, e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (MalformedObjectNameException e) {
      LOG.error("Caught an exception while processing JMX request: {}", request, e);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  private void listBeans(JsonGenerator jg, ObjectName qry, String attribute,
      HttpServletResponse response) throws IOException {
    LOG.debug("Listing beans for " + qry);
    Set<ObjectName> names = mBeanServer.queryNames(qry, null);

    jg.writeArrayFieldStart("beans");
    Iterator<ObjectName> it = names.iterator();
    while (it.hasNext()) {
      ObjectName oname = it.next();
      MBeanInfo minfo;
      String code = "";
      Object attributeinfo = null;
      try {
        minfo = mBeanServer.getMBeanInfo(oname);
        code = minfo.getClassName();
        String prs = "";
        try {
          if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
            prs = "modelerType";
            code = (String) mBeanServer.getAttribute(oname, prs);
          }
          if (attribute != null) {
            prs = attribute;
            attributeinfo = mBeanServer.getAttribute(oname, prs);
          }
        } catch (AttributeNotFoundException e) {
          // If the modelerType attribute was not found, the class name is used
          // instead.
          LOG.error("Failed to get attribute {} of {}: ", prs, oname, e);
        } catch (MBeanException e) {
          // The code inside the attribute getter threw an exception so log it,
          // and fall back on the class name
          LOG.error("Failed to get attribute {} of {}: ", prs, oname, e);
        } catch (RuntimeException e) {
          // For some reason even with an MBeanException available to them
          // Runtime exceptionscan still find their way through, so treat them
          // the same as MBeanException
          LOG.error("Failed to get attribute {} of {}: ", prs, oname, e);
        } catch (ReflectionException e) {
          // This happens when the code inside the JMX bean (setter?? from the
          // java docs) threw an exception, so log it and fall back on the
          // class name
          LOG.error("Failed to get attribute {} of {}: ", prs, oname, e);
        }
      } catch (InstanceNotFoundException e) {
        // Ignored for some reason the bean was not found so don't output it
        continue;
      } catch (IntrospectionException e) {
        // This is an internal error, something odd happened with reflection so
        // log it and don't output the bean.
        LOG.error("Problem while trying to process JMX query: {} with MBean {}", qry, oname, e);
        continue;
      } catch (ReflectionException e) {
        // This happens when the code inside the JMX bean threw an exception, so
        // log it and don't output the bean.
        LOG.error("Problem while trying to process JMX query: {} with MBean {}", qry, oname, e);
        continue;
      }

      jg.writeStartObject();
      jg.writeStringField("name", oname.toString());

      jg.writeStringField("modelerType", code);
      if ((attribute != null) && (attributeinfo == null)) {
        jg.writeStringField("result", "ERROR");
        jg.writeStringField("message", "No attribute with name " + attribute
            + " was found.");
        jg.writeEndObject();
        jg.writeEndArray();
        jg.close();
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        return;
      }

      if (attribute != null) {
        writeAttribute(jg, attribute, attributeinfo);
      } else {
        MBeanAttributeInfo[] attrs = minfo.getAttributes();
        for (int i = 0; i < attrs.length; i++) {
          writeAttribute(jg, oname, attrs[i]);
        }
      }
      jg.writeEndObject();
    }
    jg.writeEndArray();
  }

  private void writeAttribute(JsonGenerator jg, ObjectName oname, MBeanAttributeInfo attr)
      throws IOException {
    if (!attr.isReadable()) {
      return;
    }
    String attName = attr.getName();
    if ("modelerType".equals(attName)) {
      return;
    }
    if (attName.indexOf("=") >= 0 || attName.indexOf(":") >= 0
        || attName.indexOf(" ") >= 0) {
      return;
    }
    Object value = null;
    try {
      value = mBeanServer.getAttribute(oname, attName);
    } catch (RuntimeMBeanException e) {
      // UnsupportedOperationExceptions happen in the normal course of business,
      // so no need to log them as errors all the time.
      if (e.getCause() instanceof UnsupportedOperationException) {
        LOG.debug("Failed to get attribute {} of {}: ", attName, oname, e);
      } else {
        LOG.error("Failed to get attribute {} of {}: ", attName, oname, e);
      }
      return;
    } catch (RuntimeErrorException e) {
      LOG.error("Failed to get attribute {} of {}: ",
          attName, oname, e);
      return;
    } catch (AttributeNotFoundException e) {
      // Ignored the attribute was not found, which should never happen because the bean
      // just told us that it has this attribute, but if this happens just don't output
      // the attribute.
      return;
    } catch (MBeanException e) {
      // The code inside the attribute getter threw an exception so log it, and
      // skip outputting the attribute
      LOG.error("Failed to get attribute {} of {}: ", attName, oname, e);
      return;
    } catch (RuntimeException e) {
      // For some reason even with an MBeanException available to them Runtime exceptions
      // can still find their way through, so treat them the same as MBeanException
      LOG.error("Failed to get attribute {} of {}: ", attName, oname, e);
      return;
    } catch (ReflectionException e) {
      // This happens when the code inside the JMX bean (setter?? from the java docs)
      // threw an exception, so log it and skip outputting the attribute
      LOG.error("Failed to get attribute {} of {}: ", attName, oname, e);
      return;
    } catch (InstanceNotFoundException e) {
      // Ignored the mbean itself was not found, which should never happen because we
      // just accessed it (perhaps something unregistered in-between) but if this
      // happens just don't output the attribute.
      return;
    }

    writeAttribute(jg, attName, value);
  }

  private void writeAttribute(JsonGenerator jg, String attName, Object value) throws IOException {
    jg.writeFieldName(attName);
    writeObject(jg, value);
  }

  private void writeObject(JsonGenerator jg, Object value) throws IOException {
    if (value == null) {
      jg.writeNull();
    } else {
      Class<?> c = value.getClass();
      if (c.isArray()) {
        jg.writeStartArray();
        int len = Array.getLength(value);
        for (int j = 0; j < len; j++) {
          Object item = Array.get(value, j);
          writeObject(jg, item);
        }
        jg.writeEndArray();
      } else if (value instanceof Number) {
        Number n = (Number) value;
        jg.writeNumber(n.toString());
      } else if (value instanceof Boolean) {
        Boolean b = (Boolean) value;
        jg.writeBoolean(b);
      } else if (value instanceof CompositeData) {
        CompositeData cds = (CompositeData) value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        jg.writeStartObject();
        for (String key: keys) {
          writeAttribute(jg, key, cds.get(key));
        }
        jg.writeEndObject();
      } else if (value instanceof TabularData) {
        TabularData tds = (TabularData) value;
        jg.writeStartArray();
        for (Object entry : tds.values()) {
          writeObject(jg, entry);
        }
        jg.writeEndArray();
      } else {
        jg.writeString(value.toString());
      }
    }
  }
}
