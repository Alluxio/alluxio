require "erb"
require "securerandom"

module Jekyll
  module NavTabs
    class NavTabsBlock < Liquid::Block
      def render(context)
        environment = context.environments.first
        environment['navtabs'] = {} # reset each time
        super

        uuid = SecureRandom.uuid
        template = ERB.new <<-EOF
<ul class="nav nav-tabs nav-tab-margin" id="<%= uuid %>" role="tablist">
<% environment['navtabs'].each_with_index do |(key, _), index| %>
  <li class="nav-item">
    <a <%= index == 0 ? 'class="nav-link active"' : 'class="nav-link"'%> id="<%= key %>-tab" data-toggle="tab" href="#<%= key %>" role="tab" aria-controls="<%= key %>" <%= index == 0 ? 'aria-selected="true"' : 'aria-selected="false"'%>><%= key %></a>
  </li>
<% end %>
</ul>
<div class="tab-content" id="<%= uuid %>-content">
<% environment['navtabs'].each_with_index do |(key, value), index| %>
  <div <%= index == 0 ? 'class="tab-pane fade show active"' : 'class="tab-pane fade"'%> id="<%= key %>" role="tabpanel" aria-labelledby="<%= key %>-tab"><%= value %></div>
<% end %>
</div>
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
