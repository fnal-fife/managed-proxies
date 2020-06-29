Name:           managed-proxies
Version:        4.1.1
Release:        1
Summary:        Service to create VOMS proxies from service certificates and distribute them to experiment nodes

Group:          Applications/System
License:        Fermitools Software Legal Information (Modified BSD License)
URL:            https://cdcvs.fnal.gov/redmine/projects/discompsupp/wiki/MANAGEDPROXIES
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %(mktemp -ud %{_tmppath}/%{name}-%{version}-XXXXXX)

BuildArch:      x86_64

# Requires:       fermilab-util_kx509
# Requires:       cigetcert
Requires:       voms-clients
Requires:       vo-client
Requires:       openssl


%description
Service to create VOMS proxies from service certificates and distribute them to experiment nodes

%prep
test ! -d %{buildroot} || {
rm -rf %{buildroot}
}
%setup -q

%build


%install
# Config file to /etc/managed-proxies
mkdir -p %{buildroot}/%{_sysconfdir}/%{name}
install -m 0774 managedProxies.yml %{buildroot}/%{_sysconfdir}/%{name}/managedProxies.yml

# Executables to /usr/bin
mkdir -p %{buildroot}/%{_bindir}
install -m 0755 check-certs %{buildroot}/%{_bindir}/check-certs
install -m 0755 proxy-push %{buildroot}/%{_bindir}/proxy-push
install -m 0755 store-in-myproxy %{buildroot}/%{_bindir}/store-in-myproxy

# Cron and logrotate
mkdir -p %{buildroot}/%{_sysconfdir}/cron.d
install -m 0644 %{name}.cron %{buildroot}/%{_sysconfdir}/cron.d/%{name}
mkdir -p %{buildroot}/%{_sysconfdir}/logrotate.d
install -m 0644 %{name}.logrotate %{buildroot}/%{_sysconfdir}/logrotate.d/%{name}

# Templates
mkdir -p %{buildroot}/%{_datadir}/%{name}/templates
install -m 0644 templates/*.txt  %{buildroot}/%{_datadir}/%{name}/templates

# Empty dir
# mkdir -p %{buildroot}/opt/%{name}/proxies


%clean
rm -rf %{buildroot}

%files
%defattr(0755, rexbatch, fife, 0774)
%config(noreplace) %{_sysconfdir}/%{name}/managedProxies.yml
%config(noreplace) %attr(0644, root, root) %{_sysconfdir}/cron.d/%{name}
%config(noreplace) %attr(0644, root, root) %{_sysconfdir}/logrotate.d/%{name}
%{_datadir}/%{name}/templates
%{_bindir}/check-certs
%{_bindir}/proxy-push
%{_bindir}/store-in-myproxy

%post
test -d /var/log/%{name} || {
install -d /var/log/%{name} -m 0774 -o rexbatch -g fife
}

%changelog
* Tue Feb 5 2019 Shreyas Bhat <sbhat@fnal.gov> - 2.0
First version of the managed proxies RPM
