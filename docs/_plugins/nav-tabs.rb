require "erb"

module Jekyll
  module NavTabs
    class NavTabsBlock < Liquid::Block
      def initialize(tag_name, markup, tokens)
        super
        # allow spaces in the NavTabs identifier
        id = markup.strip.gsub(' ', '__')
        @navID = "nav-#{id}"
      end
      def render(context)
        environment = context.environments.first
        environment['navtabs'] = {} # reset each time
        super

        # key.gsub(' ', '__') in the template string allows for spaces in the NavTab name
        # this could cause a conflict in the unlikely scenario where someone has the two tabs: "some thing" and "some__thing"
        template = ERB.new <<-EOF
<ul class="nav nav-tabs nav-tab-margin" id="<%= @navID %>" role="tablist">
<% environment['navtabs'].each_with_index do |(key, _), index| %>
  <li class="nav-item">
    <a <%= index == 0 ? 'class="nav-link active"' : 'class="nav-link"'%> id="<%= key.gsub(' ', '__') %>-tab-<%= @navID %>" data-toggle="tab" href="#<%= key.gsub(' ', '__') %>-<%= @navID %>" role="tab" aria-controls="<%= key.gsub(' ', '__') %>-<%= @navID %>" <%= index == 0 ? 'aria-selected="true"' : 'aria-selected="false"'%>><%= key %></a>
  </li>
<% end %>
</ul>
<div class="tab-content" id="<%= @navID %>-content">
<% environment['navtabs'].each_with_index do |(key, value), index| %>
  <div <%= index == 0 ? 'class="tab-pane fade show active"' : 'class="tab-pane fade"'%> id="<%= key.gsub(' ', '__') %>-<%= @navID %>" role="tabpanel" aria-labelledby="<%= key.gsub(' ', '__') %>-tab-<%= @navID %>"><%= value %></div>
<% end %>
</div>
<hr/>
        EOF
        template.result(binding)
      end
    end

    class NavTabBlock < Liquid::Block
      alias_method :render_block, :render

      def initialize(tag_name, markup, tokens)
        super
        if markup == ""
          raise SyntaxError.new("No toggle name given in #{tag_name} tag")
        end
        @toggle = markup.strip
      end

      def render(context)
        site = context.registers[:site]
        converter = site.find_converter_instance(::Jekyll::Converters::Markdown)
        environment = context.environments.first
        environment['navtabs'] ||= {}
        environment['navtabs'][@toggle] = converter.convert(render_block(context))
      end
    end
  end
end

Liquid::Template.register_tag("navtab", Jekyll::NavTabs::NavTabBlock)
Liquid::Template.register_tag("navtabs", Jekyll::NavTabs::NavTabsBlock)
